require 'kafka'
require 'securerandom'


class Experiment
  def initialize(id)
    @id = id
  end
end

class Application
  APPS = {'315b76b5-7f78-4d0e-bb76-e47d8341540b' => [Experiment.new('567ae507-1a46-4e29-9716-eb8b534dcb35'), Experiment.new('bcc6b882-5b73-4630-a466-f7b908f84062')], 
          '0bff7188-d9b4-4223-bf61-b626c64ccc34' => [Experiment.new('461ac350-11d9-4f16-a6fa-d10e50d5e42f')]}

  def initialize(id)
    @id = id
  end

  def self.create
    id = APPS.keys.sample
    app = Application.new(id)

    experiment = APPS[id].sample
    return app,experiment
  end
end

class Event

  CLIENTS = ["cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.10, "cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.5, "cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.5, "cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.3, "cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.7, "cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.10, "cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.15, "cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.5, "cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.10, "cf8b9603-ae0c-46f9-9312-f35c1910d697"=> 0.30]

  def self.lang()
    langs = {'eng' => 0.25 ,'hin' => 0.5 ,'span' => 0.2, 'rus' => 0.05}
    acc = 0; langs.each { |e,w| langs[e] = acc+=w }
    lrand = rand()
    langs.find{ |e,w| w>lrand; }[0]
  end

  def self.m_version(os)
    # N Weighted Choice
    if os == 'ios'
      versions = {'7' => 0.25 ,'6' => 0.5 ,'5' => 0.2, '4' => 0.05}
    elsif os =='android'
      versions = {'2.4' => 0.25 ,'2.3' => 0.5 ,'2.2' => 0.2, '2.1' => 0.05}
    end
    acc = 0
    versions.each { |e,w| versions[e] = acc+=w }

    vrand = rand()
    version = versions.find{ |e,w| w>vrand; }[0]
  end
  
  def self.m_id()
  end

  def self.mobile()
    # Binary choice
    os = if rand() < 0.30 then 'ios' else 'android' end

    version = m_version(os)

    

    
    # Generate MAC address
    # mac_address = (1..6).map{"%0.2X"%rand(256)}.join(":")

    {os:os, version:version, mac_address:mac_address}
  end

    
  def self.create
    event = {}
    event[:id] = SecureRandom.uuid()
    event[:client] = {}
    event[:client][:mobile] = mobile()
    event[:client][:timestamp] = Time.now.to_i
    event[:client][:language] = lang()
    event[:application], event[:experiment] = Application.create()

    event.to_json
  end

end

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


    puts Event.create()

    
  end
end
