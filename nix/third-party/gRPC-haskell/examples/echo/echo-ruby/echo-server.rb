this_dir = File.expand_path(File.dirname(__FILE__))
$LOAD_PATH.unshift(this_dir)

require 'grpc'
require 'echo_services'

$i = 0

class EchoServer < Echo::Echo::Service
  def do_echo(echo_req, _unused_call)
    $i = $i+1
    if $i % 100 == 0
      puts($i)
    end
    return echo_req
  end
end

def main
  s = GRPC::RpcServer.new
  s.add_http2_port('0.0.0.0:50051', :this_port_is_insecure)
  s.handle(EchoServer)
  s.run_till_terminated
end

main
