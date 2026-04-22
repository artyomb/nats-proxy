require "async/http/protocol/http1/request"
require "protocol/rack/adapter/rack31"

module Async
  module HTTP
    module Protocol
      module HTTP1
        class Request
          attr_accessor :request_target, :absolute_form_target

          class << self
            # reason: preserve the raw proxy request-target before async-http normalizes it.
            def read(connection)
              connection.read_request do |authority, method, target, version, headers, body|
                request =
                  if method == ::Protocol::HTTP::Methods::CONNECT
                    new(connection, nil, target, method, nil, version, headers, body)
                  elsif valid_path?(target)
                    new(connection, nil, authority, method, target, version, headers, body)
                  elsif (match = target.match(URI_PATTERN))
                    new(connection, match[:scheme], match[:authority], method, match[:path], version, headers, body).tap do |value|
                      value.absolute_form_target = true
                    end
                  else
                    raise ::Protocol::HTTP1::BadRequest, "Invalid request target: #{target}"
                  end

                request.request_target = target
                request
              end
            end
          end
        end
      end
    end
  end
end

module Protocol
  module Rack
    module Adapter
      class Rack31
        # Rack SPEC allows CONNECT authority-form in PATH_INFO.
        # Some protocol-http/falcon request objects carry CONNECT target in `authority`
        # while `path` can be nil. This fallback keeps env construction stable.
        def make_environment(request)
          request_target =
            if request.path && !request.path.empty?
              request.path
            elsif request.respond_to?(:connect?) && request.connect?
              request.authority.to_s
            else
              "/"
            end

          request_path, query_string = request_target.split("?", 2)
          server_name, server_port = (request.authority || "").split(":", 2)

          env = {
            PROTOCOL_HTTP_REQUEST => request,

            RACK_ERRORS => $stderr,
            RACK_LOGGER => self.logger,
            RACK_RESPONSE_FINISHED => [],

            CGI::REQUEST_METHOD => request.method,
            CGI::SCRIPT_NAME => "",
            CGI::PATH_INFO => request_path,
            CGI::REQUEST_PATH => request_path,
            CGI::REQUEST_URI => request_target,
            CGI::QUERY_STRING => query_string || "",
            CGI::SERVER_PROTOCOL => request.version,
            RACK_URL_SCHEME => request.scheme,
            CGI::SERVER_NAME => server_name
          }

          env[CGI::SERVER_PORT] = server_port if server_port
          env[RACK_PROTOCOL] = request.protocol if request.protocol

          if body = request.body
            if body.empty?
              body.close
            else
              env[RACK_INPUT] = Input.new(body)
            end
          end

          self.unwrap_request(request, env)
          env
        end
      end
    end
  end
end
