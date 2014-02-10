class EventsController < ApplicationController
  skip_before_filter :verify_authenticity_token, only: [:create]

  def create
    @producer = Kafka::Producer.new(host: 'localhost', port: '9092', topic: "events")
    # event_handler.fire(params[:event])
    event = params[:event]
    
    Rails.logger.info "Pushing event #{event}"
    @producer.push(Kafka::Message.new(event))
    render status: 200, nothing: true
  end
end
