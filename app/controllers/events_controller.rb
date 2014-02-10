class EventsController < ApplicationController
  skip_before_filter :verify_authenticity_token, only: [:create]

  def create
    event_handler.fire(params[:event])
    render status: 200, nothing: true
  end
end
