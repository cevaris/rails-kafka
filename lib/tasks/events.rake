require 'kafka'
require 'securerandom'
require 'tweetstream'

require 'net/http'
require 'net/https'





namespace :events do
  desc "Tail from the Kafka log file"
  task :tail, [:topic] => :environment do |task, args|
    topic    = args[:topic].to_s
    consumer = Kafka::Consumer.new(topic: topic)

    puts "==> #{topic} <=="

    consumer.loop do |messages|
      messages.each do |message|
        puts message.inspect
        # json = JSON.parse(message.payload)
        # puts json
        # puts JSON.pretty_generate(json), "\n"
      end
    end
  end

  task :create => :environment do

    creds = JSON.parse(File.read("#{Rails.root}/.credentials.json"))
    
    TweetStream.configure do |config|
      config.consumer_key       = creds['consumer_key']
      config.consumer_secret    = creds['consumer_secret']
      config.oauth_token        = creds['oauth_token']
      config.oauth_token_secret = creds['oauth_token_secret']
      config.auth_method        = :oauth
    end

    uri = URI.parse("http://localhost:3000/events")
    # http = Net::HTTP.new(uri.host,uri.port)    

    # This will pull a sample of all tweets based on
    # your Twitter account's Streaming API role.
    TweetStream::Client.new.sample do |status|
      
      # puts "#{status.text}"

      Net::HTTP.post_form(uri, {event: status.to_json})

    end
  end
end
