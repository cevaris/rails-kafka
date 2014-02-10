require 'kafka'
require 'securerandom'
require 'tweetstream'



namespace :events do
  desc "Tail from the Kafka log file"
  task :tail, [:topic] => :environment do |task, args|
    topic    = args[:topic].to_s
    consumer = Kafka::Consumer.new(topic: topic)

    puts "==> #{topic} <=="

    consumer.loop do |messages|
      messages.each do |message|
        json = JSON.parse(message.payload)
        puts JSON.pretty_generate(json), "\n"
      end
    end
  end

  task :create, [] => :environment do |task, args|
    # consumer = Kafka::Consumer.new(topic: "events")


    # TweetStream.configure do |config|
    #   config.consumer_key       = 'abcdefghijklmnopqrstuvwxyz'
    #   config.consumer_secret    = '0123456789'
    #   config.oauth_token        = 'abcdefghijklmnopqrstuvwxyz'
    #   config.oauth_token_secret = '0123456789'
    #   config.auth_method        = :oauth
    # end

    puts RAILS_ROOT
    # This will pull a sample of all tweets based on
    # your Twitter account's Streaming API role.
    # TweetStream::Client.new.sample do |status|
    #   # The status object is a special Hash with
    #   # method access to its keys.
    #   puts "#{status.text}"
    # end


    puts Event.create()

    
  end
end
