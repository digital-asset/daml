from simple_pb2 import *
from uuid import uuid4
import random
import Queue

print "Starting python client"

channel = beta_implementations.insecure_channel('localhost', 50051)
stub = beta_create_SimpleService_stub(channel)

def runTests():
    # Test normal call: return a sum of all numbers sent to it
    print "Test 100 random sums"
    for i in xrange(100):
        randints = [random.randint(0, 1000) for _ in xrange(random.randint(10, 1000))]
        name = "test%d" % i
        response = stub.normalCall(SimpleServiceRequest(request = name, num = randints), 10)
        assert response.response == name
        assert response.num == sum(randints)

    # Test streaming call: The server response will be the sum of all numbers sent in the request along with a concatenation of the request name
    print "Test 100 random sums (client streaming)"
    for i in xrange(100):
        # avoid globals
        expected_sum = [0]
        expected_response_name = ['']
        def send_requests(esum, ename):
            for _ in xrange(random.randint(5, 50)):
                nums = [random.randint(0, 1000) for _ in xrange(random.randint(10, 100))]
                name = str(uuid4())
                esum[0] += sum(nums)
                ename[0] += name
                yield SimpleServiceRequest(request = name, num = nums)
        response = stub.clientStreamingCall(send_requests(expected_sum, expected_response_name), 10)
        assert response.response == expected_response_name[0]
        assert response.num == expected_sum[0]

    # Test server streaming call: The server should respond once for each number in the request
    print "Test 100 random server streaming calls"
    for i in xrange(100):
        nums = [random.randint(0, 1000) for _ in xrange(random.randint(0, 1000))]

        for response in stub.serverStreamingCall(SimpleServiceRequest(request = "server streaming", num = nums), 60):
            assert response.num == nums[0]
            assert response.response == "server streaming"
            nums = nums[1:]

    # Test bidirectional streaming: for each request, we should get a response indicating the sum of all numbers sent in the last request
    print "Test bidirectional streaming"
    for i in xrange(100):
        requests = Queue.Queue()
        def send_requests():
            for _ in xrange(random.randint(5, 50)):
                nums = [random.randint(0, 1000) for _ in xrange(random.randint(10, 100))]
                name = str(uuid4())

                requests.put((name, sum(nums)))

                yield SimpleServiceRequest(request = name, num = nums)

        for response in stub.biDiStreamingCall(send_requests(), 10):
            (exp_name, exp_sum) = requests.get()

            assert response.response == exp_name
            assert response.num == exp_sum

    print "Sending DONE message to server"
    stub.done(SimpleServiceDone(), 10)

    print "All python client tests against the generated server were successful"

try:
    runTests()
except Exception as e:
    print "Python client test exception caught: "
    print e.__doc__
    print e.message
    print "Sending DONE message to server before exiting"
    stub.done(SimpleServiceDone(), 10)
    print "Exiting with failure status"
    exit(1)
