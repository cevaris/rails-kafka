require "singleton"

class KafkaEvents
  include Singleton

  def initialize
    @queue = Queue.new
  end

  def write(messages)
    puts 'Writing messages to queue'
    @queue.push(messages)
  end

  def start(producer)
    Thread.new do
      while true
        batch = @queue.pop
        producer.batch do
          batch.each do |message|
            Rails.logger.info "PreKafka - #{message}"
            producer.send(Kafka::Message.new(message))
          end
        end
      end
    end
  end
end
