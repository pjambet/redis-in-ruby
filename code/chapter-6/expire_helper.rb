module BYORedis
  module ExpireHelper

    def self.check_if_expired(data_store, expires, key)
      expires_entry = expires[key]
      if expires_entry && expires_entry < Time.now.to_f * 1000
        logger.debug "evicting #{ key }"
        expires.delete(key)
        data_store.delete(key)
      end
    end

    def self.logger
      @logger ||= Logger.new(STDOUT).tap do |l|
        l.level = LOG_LEVEL
      end
    end
  end
end
