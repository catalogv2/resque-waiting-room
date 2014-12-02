module Resque
  module Plugins
    module WaitingRoom
      class MissingParams < RuntimeError; end

      def can_be_performed(params)
        raise MissingParams unless params.is_a?(Hash) && params.keys.sort {|x,y| x.to_s <=> y.to_s } == [:period, :times]

        @period ||= params[:period]
        @max_performs ||= params[:times].to_i
      end

      def waiting_room_redis_key
        [self.to_s, 'remaining_performs'].compact.join(':')
      end
       
      def before_perform_waiting_room(*args)
        key = waiting_room_redis_key
        return unless remaining_performs_key?(key)
        performs_left = Resque.redis.decrby(key, 1).to_i
        puts "performs_left #{performs_left}" 
        if performs_left < 1
          Resque.push 'waiting_room', :class => self.to_s, :args => args
          raise Resque::Job::DontPerform
        end
      end

      def remaining_performs_key?(key)
        # if true then we will only set a key if it doesn't exist
        # if false then we will set the key regardless.  If we have
        # a negative number then redis either doesn't know about the key
        # or it doesn't have a ttl, either way we want to create a new key
        # with a new ttl.
        create_new_key = Resque.redis.ttl(key).to_i < 0

        # Redis SET: with the ex and nx option  sets the keys if it doesn't exist,
        # returns true if key was created redis => 2.6 required
        # http://redis.io/commands/SET
      
        res = false
        begin
          Resque.redis.watch(key) do 
            value = Resque.redis.get(key) 
            Resque.redis.multi do
              if value.nil? || create_new_key 
                Resque.redis.setex(key, @period,@max_performs - 1)
                res = false
              else
                res = true
              end
            end
          end
        rescue 
          res = false # if the value is modified by another job, we put this job in the waiting_room.
        end
        return res
      end

      def repush(*args)
        key = waiting_room_redis_key
        value = Resque.redis.get(key)
        no_performs_left = value && value != '' && value.to_i <= 0
        Resque.push 'waiting_room', :class => self.to_s, :args => args if no_performs_left

        return no_performs_left
      end


      #method to get the test matcher working on ruby 1.8.7
      def is_instance_of_waiting_room_plugin?
        true
      end
    end
  end
end
