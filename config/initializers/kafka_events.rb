require 'kafka'


producer = Kafka::Producer.new(topic: "events")
KafkaEvents.instance.start(producer)
