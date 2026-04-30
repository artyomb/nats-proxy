# frozen_string_literal: true

require "socket"
require "securerandom"
require "tempfile"
require "tmpdir"
require "fileutils"
require "zlib"
require "rubygems/package"

module NatsServerHelper
  def project_path = File.expand_path("../..", __dir__)
  def server_path = File.join(project_path, "spec", "bin", "nats-server")
  def server_archive_path = File.join(project_path, "spec", "bin", "nats-server-linux-amd64.tar.gz")
  def server_extract_lock_path = File.join(Dir.tmpdir, "nats-proxy-nats-server-extract.lock")
  def nats_server = RSpec.current_example.metadata.fetch(:nats_server_context)
  def nats_url = nats_server.fetch(:url)

  def ensure_server_binary!
    return server_path if File.exist?(server_path) && File.executable?(server_path)

    unless File.exist?(server_archive_path)
      raise "missing bundled nats-server binary and archive at #{server_archive_path}"
    end

    File.open(server_extract_lock_path, "w") do |lock_file|
      lock_file.flock(File::LOCK_EX)
      return server_path if File.exist?(server_path) && File.executable?(server_path)

      extract_server_archive!
    end

    server_path
  end

  def extract_server_archive!
    tmp_output = "#{server_path}.tmp-#{Process.pid}-#{Thread.current.object_id}"

    begin
      Zlib::GzipReader.open(server_archive_path) do |gzip_io|
        Gem::Package::TarReader.new(gzip_io) do |tar|
          tar.each do |entry|
            next unless entry.file?
            next unless File.basename(entry.full_name) == "nats-server"

            File.open(tmp_output, "wb") { |file| IO.copy_stream(entry, file) }
            FileUtils.chmod(0o755, tmp_output)
            FileUtils.mv(tmp_output, server_path)
            return server_path
          end
        end
      end
    ensure
      FileUtils.rm_f(tmp_output)
    end

    raise "archive #{server_archive_path} does not contain nats-server"
  end

  def free_port
    server = TCPServer.new("127.0.0.1", 0)
    server.addr[1]
  ensure
    server&.close
  end

  def wait_for_server(port, server_pid, log_path, timeout: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

    loop do
      socket = TCPSocket.new("127.0.0.1", port)
      socket.close
      return
    rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH
      if Process.waitpid(server_pid, Process::WNOHANG)
        raise <<~MSG
          bundled nats-server exited before becoming ready
          --- nats-server log ---
          #{File.exist?(log_path) ? File.read(log_path) : "(log file missing)"}
          --- end nats-server log ---
        MSG
      end

      if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        raise <<~MSG
          bundled nats-server did not start on port #{port}
          --- nats-server log ---
          #{File.exist?(log_path) ? File.read(log_path) : "(log file missing)"}
          --- end nats-server log ---
        MSG
      end

      sleep 0.1
    end
  end

  def start_server_for(context)
    ensure_server_binary!

    argv = [
      server_path,
      "-c", context.fetch(:config_path),
      "-p", context.fetch(:port).to_s
    ]

    if context.fetch(:cli_jetstream, true)
      argv.insert(3, "-js", "-sd", context.fetch(:store_dir))
    end

    context[:pid] = Process.spawn(
      *argv,
      out: context.fetch(:log),
      err: context.fetch(:log),
      chdir: project_path
    )

    wait_for_server(context.fetch(:port), context.fetch(:pid), context.fetch(:log_path))
    context.fetch(:pid)
  end

  def stop_server(pid)
    return unless pid

    Process.kill("TERM", pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Process::Waiter::Error
    nil
  end

  def stop_current_server
    stop_server(nats_server[:pid])
    nats_server[:pid] = nil
  end

  def with_nats_server
    port = free_port

    Tempfile.create(["nats-proxy", ".log"]) do |log|
      Tempfile.create(["nats-proxy", ".conf"]) do |config|
        config.write("debug: true\ntrace: true\n")
        config.flush

        Dir.mktmpdir("nats-proxy-js") do |store_dir|
          context = {
            port: port,
            url: "nats://127.0.0.1:#{port}",
            log: log,
            log_path: log.path,
            config_path: config.path,
            store_dir: store_dir
          }

          begin
            start_server_for(context)
            yield context
          ensure
            stop_server(context[:pid])
          end
        end
      end
    end
  end

  def with_leaf_topology_nats
    attempts = 0

    begin
      port = free_port
      local_user = "local_#{SecureRandom.hex(4)}"
      local_password = SecureRandom.hex(10)
      proxy_user = "proxy_#{SecureRandom.hex(4)}"
      proxy_password = SecureRandom.hex(10)

      Tempfile.create(["nats-proxy-leaf", ".log"]) do |log|
        Tempfile.create(["nats-proxy-leaf", ".conf"]) do |config|
          Dir.mktmpdir("nats-proxy-leaf-js") do |store_dir|
            config.write(leaf_topology_config(
              local_user:,
              local_password:,
              proxy_user:,
              proxy_password:,
              store_dir:
            ))
            config.flush

            context = {
              port:,
              url: "nats://127.0.0.1:#{port}",
              local_url: "nats://#{local_user}:#{local_password}@127.0.0.1:#{port}",
              proxy_url: "nats://#{proxy_user}:#{proxy_password}@127.0.0.1:#{port}",
              log: log,
              log_path: log.path,
              config_path: config.path,
              store_dir:,
              cli_jetstream: false
            }

            begin
              start_server_for(context)
              return yield context
            ensure
              stop_server(context[:pid])
            end
          end
        end
      end
    rescue RuntimeError => e
      attempts += 1
      retry if e.message.include?("import forms a cycle") && attempts < 3

      raise
    end
  end

  def leaf_topology_config(local_user:, local_password:, proxy_user:, proxy_password:, store_dir:)
    <<~CONF
      debug: true
      trace: true

      jetstream {
        store_dir: "#{store_dir}"
      }

      accounts: {
        local: {
          users: [
            { user: "#{local_user}", password: "#{local_password}" }
          ]
          exports: [
            { stream: "to.proxy.requests.>" }
            { stream: "to.proxy.sessions.upstream.>" }
            { stream: "to.proxy.cancel.>" }
            { service: "_INBOX.>", accounts: [proxy] }
          ]
          imports: [
            { stream: { account: proxy, subject: "proxy.responses.>" }, to: "from.proxy.responses.>" }
            { stream: { account: proxy, subject: "proxy.sessions.downstream.>" }, to: "from.proxy.sessions.downstream.>" }
            { service: { account: proxy, subject: "$JS.API.>" }, to: "JS.PROXY.API.>" }
            { service: { account: proxy, subject: "$JS.ACK.>" } }
          ]
        }

        proxy: {
          users: [
            { user: "#{proxy_user}", password: "#{proxy_password}" }
          ]
          exports: [
            { stream: "proxy.responses.>" }
            { stream: "proxy.sessions.downstream.>" }
            { service: "$JS.API.>", accounts: [local] }
            { service: "$JS.ACK.>", accounts: [local] }
          ]
          imports: [
            { stream: { account: local, subject: "to.proxy.requests.>" }, to: "proxy.requests.>" }
            { stream: { account: local, subject: "to.proxy.sessions.upstream.>" }, to: "proxy.sessions.upstream.>" }
            { stream: { account: local, subject: "to.proxy.cancel.>" }, to: "proxy.cancel.>" }
            { service: { account: local, subject: "_INBOX.>" }, to: "_INBOX.>" }
          ]
          jetstream: enabled
        }
      }
    CONF
  end
end

if defined?(RSpec)
  RSpec.configure do |config|
    config.include NatsServerHelper

    config.around(:example, :nats_server) do |example|
      with_nats_server do |context|
        example.metadata[:nats_server_context] = context
        example.run
      ensure
        example.metadata.delete(:nats_server_context)
      end
    end
  end
end
