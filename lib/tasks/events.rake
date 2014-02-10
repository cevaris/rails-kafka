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

  task :create => :environment do

    creds = JSON.parse(File.read("#{Rails.root}/.credentials.json"))
    # consumer = Kafka::Consumer.new(topic: "events")

    TweetStream.configure do |config|
      config.consumer_key       = creds['consumer_key']
      config.consumer_secret    = creds['consumer_secret']
      config.oauth_token        = creds['oauth_token']
      config.oauth_token_secret = creds['oauth_token_secret']
      config.auth_method        = :oauth
    end

    # This will pull a sample of all tweets based on
    # your Twitter account's Streaming API role.
    TweetStream::Client.new.sample do |status|
      # The status object is a special Hash with
      # method access to its keys.
      puts "#{status.text}"
    end


    
  end
end
