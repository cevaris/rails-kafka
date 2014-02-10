class ApplicationController < ActionController::Base
  # Prevent CSRF attacks by raising an exception.
  # For APIs, you may want to use :null_session instead.
  protect_from_forgery with: :exception


  after_filter :flush_events_to_log

  private

  def event_handler
    @event_handler ||= EventHandler.new(KafkaLog.instance)
  end

  def flush_events_to_log
    event_handler.flush
  end
  
end
