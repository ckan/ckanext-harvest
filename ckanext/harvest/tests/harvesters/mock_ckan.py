import os

import SimpleHTTPServer
import SocketServer
from threading import Thread


PORT = 8998


class MockCkanHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):
        # remove version numbers from the path
        self.path = self.path.replace('/2/', '/')
        self.path = self.path.replace('/3/', '/')
        if self.path == '/':
            self.path = '/simplehttpwebpage_content.html'
        return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)


def serve(port=PORT):
    '''Runs a CKAN-alike app (over HTTP) that is used for harvesting tests'''

    # Make sure we serve from the tests' XML directory
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          'mock_ckan_files'))

    class TestServer(SocketServer.TCPServer):
        allow_reuse_address = True

    httpd = TestServer(("", PORT), MockCkanHandler)

    print 'Serving test HTTP server at port', PORT

    httpd_thread = Thread(target=httpd.serve_forever)
    httpd_thread.setDaemon(True)
    httpd_thread.start()

