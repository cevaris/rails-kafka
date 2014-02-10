require "singleton"

class KafkaEvents
  include Singleton

  def initialize
    @queue = Queue.new
  end

  def write(messages)
    @queue.push(messages)
  end

  def start(producer)
    Thread.new do
      while batch = @queue.pop
        producer.batch do
          batch.each do |message|
            puts "PreKafka - #{message}"
            producer.push(Kafka::Message.new(message))
          end
        end
      end
    end

  end
end
