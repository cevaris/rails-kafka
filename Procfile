web: bundle exec rails server -p $PORT -e development
resque: env TERM_CHILD=1 VERBOSE=true QUEUE=* INTERVAL=5 COUNT='2' bundle exec rake environment resque:workers