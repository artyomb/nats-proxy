require 'json'
require 'stack-service-base'
require 'sinatra'
require 'sequel'

StackServiceBase.rack_setup self

DB ||= Sequel.connect ENV.fetch('DB_URL') if ENV['DB_URL']

# require Models ...
# Dir["#{__dir__}/models/*"].each { require_relative _1 }

get '/', &-> { slim :index }

get '/api/info' do
  content_type :json
  {
    service: ENV.fetch('STACK_SERVICE_NAME', 'nats-proxy'),
    status: 'ok'
  }.to_json
end

run Sinatra::Application
