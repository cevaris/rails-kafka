class EventHandler
  def initialize(logger)
    @logger   = logger
    @messages = []
  end

  def fire(event)
    @messages.push(event)
  end

  def flush
    @logger.write(@messages) if @messages.present?
  end
end
