#include <TestSupport.h>
#include <Core/UnionStation/Core.h>
#include <Core/UnionStation/Transaction.h>
#include <MessageClient.h>
#include <UstRouter/Controller.h>
#include <Utils/MessageIO.h>
#include <Utils/ScopeGuard.h>

#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <oxt/thread.hpp>
#include <set>

using namespace Passenger;
using namespace Passenger::UnionStation;
using namespace std;
using namespace oxt;

namespace tut {
	struct Core_UnionStationTest {
		static const unsigned long long YESTERDAY = 1263299017000000ull;  // January 12, 2009, 12:23:37 UTC
		static const unsigned long long TODAY     = 1263385422000000ull;  // January 13, 2009, 12:23:42 UTC
		static const unsigned long long TOMORROW  = 1263471822000000ull;  // January 14, 2009, 12:23:42 UTC
		#define TODAY_TXN_ID "cjb8n-abcd"
		#define TODAY_TIMESTAMP_STR "cftz90m3k0"

		boost::shared_ptr<BackgroundEventLoop> bg;
		boost::shared_ptr<ServerKit::Context> context;
		TempDir tmpdir;
		string socketFilename;
		string socketAddress;
		FileDescriptor serverFd;
		VariantMap controllerOptions;
		boost::shared_ptr<UstRouter::Controller> controller;
		CorePtr core, core2, core3, core4;

		Core_UnionStationTest()
			: tmpdir("tmp.union_station")
		{
			socketFilename = tmpdir.getPath() + "/socket";
			socketAddress = "unix:" + socketFilename;
			setLogLevel(LVL_ERROR);

			controllerOptions.set("ust_router_username", "test");
			controllerOptions.set("ust_router_password", "1234");
			controllerOptions.setBool("ust_router_dev_mode", true);
			controllerOptions.set("ust_router_dump_dir", tmpdir.getPath());

			core = boost::make_shared<Core>(socketAddress, "test", "1234",
				"localhost");
			core2 = boost::make_shared<Core>(socketAddress, "test", "1234",
				"localhost");
			core3 = boost::make_shared<Core>(socketAddress, "test", "1234",
				"localhost");
			core4 = boost::make_shared<Core>(socketAddress, "test", "1234",
				"localhost");
		}

		~Core_UnionStationTest() {
			// Silence error disconnection messages during shutdown.
			setLogLevel(LVL_CRIT);
			shutdown();
			SystemTime::releaseAll();
			setLogLevel(DEFAULT_LOG_LEVEL);
		}

		void init() {
			bg = boost::make_shared<BackgroundEventLoop>(false, true);
			context = boost::make_shared<ServerKit::Context>(bg->safe, bg->libuv_loop);
			serverFd.assign(createUnixServer(socketFilename.c_str(), 0, true, __FILE__, __LINE__), NULL, 0);
			controller = make_shared<UstRouter::Controller>(context.get(), controllerOptions);
			controller->listen(serverFd);
			bg->start();
		}

		void shutdown() {
			if (bg != NULL) {
				bg->safe->runSync(boost::bind(&UstRouter::Controller::shutdown, controller.get(), true));
				while (getControllerState() != UstRouter::Controller::FINISHED_SHUTDOWN) {
					syscalls::usleep(1000000);
				}
				controller.reset();
				bg->stop();
				bg.reset();
				context.reset();
				serverFd.close();
			}
		}

		UstRouter::Controller::State getControllerState() {
			UstRouter::Controller::State result;
			bg->safe->runSync(boost::bind(&Core_UnionStationTest::_getControllerState,
				this, &result));
			return result;
		}

		void _getControllerState(UstRouter::Controller::State *state) {
			*state = controller->serverState;
		}

		string timestampString(unsigned long long timestamp) {
			char str[2 * sizeof(unsigned long long) + 1];
			integerToHexatri<unsigned long long>(timestamp, str);
			return str;
		}

		MessageClient createConnection(bool sendInitCommand = true) {
			MessageClient client;
			vector<string> args;
			client.connect(socketAddress, "test", "1234");
			if (sendInitCommand) {
				client.write("init", "localhost", NULL);
				client.read(args);
			}
			return client;
		}

		string readDumpFile(const string &category = "requests") {
			return readAll(getDumpFilePath(category));
		}

		string getDumpFilePath(const string &category = "requests") {
			return tmpdir.getPath() + "/" + category;
		}
	};

	DEFINE_TEST_GROUP(Core_UnionStationTest);


	/*********** Logging interface tests ***********/

	TEST_METHOD(1) {
		// Test logging of new transaction.
		init();
		SystemTime::forceAll(YESTERDAY);

		TransactionPtr log = core->newTransaction("foobar");
		log->message("hello");
		log->message("world");
		log->flushToDiskAfterClose(true);

		ensure(!core->isNull());
		ensure(!log->isNull());

		log.reset();

		string data = readDumpFile();
		ensure(data.find("hello\n") != string::npos);
		ensure(data.find("world\n") != string::npos);
	}

	TEST_METHOD(2) {
		// Test logging of existing transaction.
		init();
		SystemTime::forceAll(YESTERDAY);

		TransactionPtr log = core->newTransaction("foobar");
		log->message("message 1");
		log->flushToDiskAfterClose(true);

		TransactionPtr log2 = core2->continueTransaction(log->getTxnId(),
			log->getGroupName(), log->getCategory());
		log2->message("message 2");
		log2->flushToDiskAfterClose(true);

		log.reset();
		log2.reset();

		string data = readDumpFile();
		ensure("(1)", data.find("message 1\n") != string::npos);
		ensure("(2)", data.find("message 2\n") != string::npos);
	}

	TEST_METHOD(3) {
		// Test logging with different points in time.
		init();
		SystemTime::forceAll(YESTERDAY);

		TransactionPtr log = core->newTransaction("foobar");
		log->message("message 1");
		SystemTime::forceAll(TODAY);
		log->message("message 2");
		log->flushToDiskAfterClose(true);

		SystemTime::forceAll(TOMORROW);
		TransactionPtr log2 = core2->continueTransaction(log->getTxnId(),
			log->getGroupName(), log->getCategory());
		log2->message("message 3");
		log2->flushToDiskAfterClose(true);

		TransactionPtr log3 = core3->newTransaction("foobar");
		log3->message("message 4");
		log3->flushToDiskAfterClose(true);

		log.reset();
		log2.reset();
		log3.reset();

		string data = readDumpFile();
		ensure("(1)", data.find(timestampString(YESTERDAY) + " 1 message 1\n") != string::npos);
		ensure("(2)", data.find(timestampString(TODAY) + " 2 message 2\n") != string::npos);
		ensure("(3)", data.find(timestampString(TOMORROW) + " 4 message 3\n") != string::npos);
		ensure("(4)", data.find(timestampString(TOMORROW) + " 1 message 4\n") != string::npos);
	}

	TEST_METHOD(4) {
		// newTransaction() and continueTransaction() write an ATTACH message
		// to the log file, while UnionStation::Transaction writes a DETACH message upon
		// destruction.
		init();
		SystemTime::forceAll(YESTERDAY);

		TransactionPtr log = core->newTransaction("foobar");

		SystemTime::forceAll(TODAY);
		TransactionPtr log2 = core2->continueTransaction(log->getTxnId(),
			log->getGroupName(), log->getCategory());
		log2->flushToDiskAfterClose(true);
		log2.reset();

		SystemTime::forceAll(TOMORROW);
		log->flushToDiskAfterClose(true);
		log.reset();

		string data = readDumpFile();
		ensure("(1)", data.find(timestampString(YESTERDAY) + " 0 ATTACH\n") != string::npos);
		ensure("(2)", data.find(timestampString(TODAY) + " 1 ATTACH\n") != string::npos);
		ensure("(3)", data.find(timestampString(TODAY) + " 2 DETACH\n") != string::npos);
		ensure("(4)", data.find(timestampString(TOMORROW) + " 3 DETACH\n") != string::npos);
	}

	TEST_METHOD(5) {
		// newTransaction() generates a new ID, while continueTransaction()
		// reuses the ID.
		init();

		TransactionPtr log = core->newTransaction("foobar");
		TransactionPtr log2 = core2->newTransaction("foobar");
		TransactionPtr log3 = core3->continueTransaction(log->getTxnId(),
			log->getGroupName(), log->getCategory());
		TransactionPtr log4 = core4->continueTransaction(log2->getTxnId(),
			log2->getGroupName(), log2->getCategory());

		ensure_equals(log->getTxnId(), log3->getTxnId());
		ensure_equals(log2->getTxnId(), log4->getTxnId());
		ensure(log->getTxnId() != log2->getTxnId());
	}

	TEST_METHOD(6) {
		// An empty UnionStation::Transaction doesn't do anything.
		init();

		UnionStation::Transaction log;
		ensure(log.isNull());
		log.message("hello world");
		ensure_equals(getFileType(getDumpFilePath()), FT_NONEXISTANT);
	}

	TEST_METHOD(7) {
		// An empty UnionStation::Core doesn't do anything.
		UnionStation::Core core;
		init();
		ensure(core.isNull());

		TransactionPtr log = core.newTransaction("foo");
		ensure(log->isNull());
		log->message("hello world");
		ensure_equals(getFileType(getDumpFilePath()), FT_NONEXISTANT);
	}

	TEST_METHOD(11) {
		// newTransaction() does not reconnect to the server for a short
		// period of time if connecting failed
		init();
		core->setReconnectTimeout(60 * 1000000);

		SystemTime::forceAll(TODAY);
		shutdown();
		ensure(core->newTransaction("foobar")->isNull());

		SystemTime::forceAll(TODAY + 30 * 1000000);
		init();
		ensure(core->newTransaction("foobar")->isNull());

		SystemTime::forceAll(TODAY + 61 * 1000000);
		ensure(!core->newTransaction("foobar")->isNull());
	}

	TEST_METHOD(12) {
		// If the UstRouter crashed and was restarted then
		// newTransaction() and continueTransaction() print a warning and return
		// a null log object. One of the next newTransaction()/continueTransaction()
		// calls will reestablish the connection when the connection timeout
		// has passed.
		init();
		SystemTime::forceAll(TODAY);
		TransactionPtr log, log2;

		log = core->newTransaction("foobar");
		core2->continueTransaction(log->getTxnId(), "foobar");
		log.reset(); // Check connection back into the pool.
		shutdown();
		init();

		log = core->newTransaction("foobar");
		ensure("(1)", log->isNull());
		log2 = core2->continueTransaction("some-id", "foobar");
		ensure("(2)", log2->isNull());

		SystemTime::forceAll(TODAY + 60000000);
		log = core->newTransaction("foobar");
		ensure("(3)", !log->isNull());
		log2 = core2->continueTransaction(log->getTxnId(), "foobar");
		ensure("(4)", !log2->isNull());
		log2->message("hello");
		log2->flushToDiskAfterClose(true);
		log.reset();
		log2.reset();

		EVENTUALLY(3,
			result = readDumpFile().find("hello\n") != string::npos;
		);
	}

	TEST_METHOD(13) {
		// continueTransaction() does not reconnect to the server for a short
		// period of time if connecting failed
		init();
		core->setReconnectTimeout(60 * 1000000);
		core2->setReconnectTimeout(60 * 1000000);

		SystemTime::forceAll(TODAY);
		TransactionPtr log = core->newTransaction("foobar");
		ensure("(1)", !log->isNull());
		ensure("(2)", !core2->continueTransaction(log->getTxnId(), "foobar")->isNull());
		shutdown();
		ensure("(3)", core2->continueTransaction(log->getTxnId(), "foobar")->isNull());

		SystemTime::forceAll(TODAY + 30 * 1000000);
		init();
		ensure("(3)", core2->continueTransaction(log->getTxnId(), "foobar")->isNull());

		SystemTime::forceAll(TODAY + 61 * 1000000);
		ensure("(4)", !core2->continueTransaction(log->getTxnId(), "foobar")->isNull());
	}

	TEST_METHOD(14) {
		// If a client disconnects from the UstRouter then all its
		// transactions that are no longer referenced and have crash protection enabled
		// will be closed and written to the sink.
		init();

		MessageClient client1 = createConnection();
		MessageClient client2 = createConnection();
		MessageClient client3 = createConnection();
		vector<string> args;

		SystemTime::forceAll(TODAY);

		client1.write("openTransaction",
			TODAY_TXN_ID, "foobar", "", "requests", TODAY_TIMESTAMP_STR,
			"-", "true", "true", NULL);
		client1.read(args);
		client2.write("openTransaction",
			TODAY_TXN_ID, "foobar", "", "requests", TODAY_TIMESTAMP_STR,
			"-", "true", NULL);
		client2.write("log", TODAY_TXN_ID, "1000", NULL);
		client2.writeScalar("hello world");
		client2.write("flush", NULL);
		client2.read(args);
		client2.disconnect();
		SHOULD_NEVER_HAPPEN(100,
			// Transaction still has references open, so should not yet be written to sink.
			result = readDumpFile().find("hello world") != string::npos;
		);

		client1.disconnect();
		client3.write("flush", NULL);
		client3.read(args);
		EVENTUALLY(5,
			result = readDumpFile().find("hello world") != string::npos;
		);
	}

	TEST_METHOD(15) {
		// If a client disconnects from the UstRouter then all its
		// transactions that are no longer referenced and don't have crash
		// protection enabled will be closed and discarded.
		init();

		MessageClient client1 = createConnection();
		MessageClient client2 = createConnection();
		MessageClient client3 = createConnection();
		vector<string> args;

		SystemTime::forceAll(TODAY);

		client1.write("openTransaction",
			TODAY_TXN_ID, "foobar", "", "requests", TODAY_TIMESTAMP_STR,
			"-", "false", "true", NULL);
		client1.read(args);
		client2.write("openTransaction",
			TODAY_TXN_ID, "foobar", "", "requests", TODAY_TIMESTAMP_STR,
			"-", "false", NULL);
		client2.write("flush", NULL);
		client2.read(args);
		client2.disconnect();
		client1.disconnect();
		client3.write("flush", NULL);
		client3.read(args);
		SHOULD_NEVER_HAPPEN(500,
			result = fileExists(getDumpFilePath()) && !readDumpFile().empty();
		);
	}

	TEST_METHOD(16) {
		// Upon server shutdown, all transaction that have crash protection enabled
		// will be closed and written to to the sink.
		init();

		MessageClient client1 = createConnection();
		MessageClient client2 = createConnection();
		vector<string> args;

		SystemTime::forceAll(TODAY);

		client1.write("openTransaction",
			TODAY_TXN_ID, "foobar", "", "requests", TODAY_TIMESTAMP_STR,
			"-", "true", "true", NULL);
		client1.read(args);
		client2.write("openTransaction",
			TODAY_TXN_ID, "foobar", "", "requests", TODAY_TIMESTAMP_STR,
			"-", "true", NULL);
		client2.write("flush", NULL);
		client2.read(args);

		shutdown();
		EVENTUALLY(5,
			result = fileExists(getDumpFilePath()) && !readDumpFile().empty();
		);
	}

	TEST_METHOD(17) {
		// Upon server shutdown, all transaction that don't have crash protection
		// enabled will be discarded.
		init();

		MessageClient client1 = createConnection();
		MessageClient client2 = createConnection();
		vector<string> args;

		SystemTime::forceAll(TODAY);

		client1.write("openTransaction",
			TODAY_TXN_ID, "foobar", "", "requests", TODAY_TIMESTAMP_STR,
			"-", "false", "true", NULL);
		client1.read(args);
		client2.write("openTransaction",
			TODAY_TXN_ID, "foobar", "", "requests", TODAY_TIMESTAMP_STR,
			"-", "false", NULL);
		client2.write("flush", NULL);
		client2.read(args);

		shutdown();
		SHOULD_NEVER_HAPPEN(200,
			result = fileExists(getDumpFilePath()) && !readDumpFile().empty();
		);
	}

	TEST_METHOD(18) {
		// Test DataStoreId
		init();
		{
			// Empty construction.
			DataStoreId id;
			ensure_equals(id.getGroupName(), "");
			ensure_equals(id.getNodeName(), "");
			ensure_equals(id.getCategory(), "");
		}
		{
			// Normal construction.
			DataStoreId id("ab", "cd", "ef");
			ensure_equals(id.getGroupName(), "ab");
			ensure_equals(id.getNodeName(), "cd");
			ensure_equals(id.getCategory(), "ef");
		}
		{
			// Copy constructor.
			DataStoreId id("ab", "cd", "ef");
			DataStoreId id2(id);
			ensure_equals(id2.getGroupName(), "ab");
			ensure_equals(id2.getNodeName(), "cd");
			ensure_equals(id2.getCategory(), "ef");
		}
		{
			// Assignment operator.
			DataStoreId id("ab", "cd", "ef");
			DataStoreId id2;
			id2 = id;
			ensure_equals(id2.getGroupName(), "ab");
			ensure_equals(id2.getNodeName(), "cd");
			ensure_equals(id2.getCategory(), "ef");

			DataStoreId id3("gh", "ij", "kl");
			id3 = id;
			ensure_equals(id3.getGroupName(), "ab");
			ensure_equals(id3.getNodeName(), "cd");
			ensure_equals(id3.getCategory(), "ef");
		}
		{
			// < operator
			DataStoreId id, id2;
			ensure(!(id < id2));

			id = DataStoreId("ab", "cd", "ef");
			id2 = DataStoreId("ab", "cd", "ef");
			ensure(!(id < id2));

			id = DataStoreId("ab", "cd", "ef");
			id2 = DataStoreId("bb", "cd", "ef");
			ensure(id < id2);

			id = DataStoreId("ab", "cd", "ef");
			id2 = DataStoreId();
			ensure(id2 < id);

			id = DataStoreId();
			id2 = DataStoreId("ab", "cd", "ef");
			ensure(id < id2);
		}
		{
			// == operator
			ensure(DataStoreId() == DataStoreId());
			ensure(DataStoreId("ab", "cd", "ef") == DataStoreId("ab", "cd", "ef"));
			ensure(!(DataStoreId("ab", "cd", "ef") == DataStoreId()));
			ensure(!(DataStoreId("ab", "cd", "ef") == DataStoreId("ab", "cd", "e")));
			ensure(!(DataStoreId("ab", "cd", "ef") == DataStoreId("ab", "c", "ef")));
			ensure(!(DataStoreId("ab", "cd", "ef") == DataStoreId("a", "cd", "ef")));
		}
	}

	TEST_METHOD(22) {
		// The destructor flushes all data.
		init();

		TransactionPtr log = core->newTransaction("foobar");
		log->message("hello world");
		log.reset();
		shutdown();

		struct stat buf;
		ensure_equals(stat(getDumpFilePath().c_str(), &buf), 0);
		ensure(buf.st_size > 0);
	}

	TEST_METHOD(23) {
		// The 'flush' command flushes all data.
		init();
		SystemTime::forceAll(YESTERDAY);

		TransactionPtr log = core->newTransaction("foobar");
		log->message("hello world");
		log.reset();

		ConnectionPtr connection = core->checkoutConnection();
		vector<string> args;
		writeArrayMessage(connection->fd, "flush", NULL);
		ensure("(1)", readArrayMessage(connection->fd, args));
		ensure_equals("(2)", args.size(), 2u);
		ensure_equals("(3)", args[0], "status");
		ensure_equals("(4)", args[1], "ok");

		struct stat buf;
		ensure_equals("(5)", stat(getDumpFilePath().c_str(), &buf), 0);
		ensure("(6)", buf.st_size > 0);
	}

	TEST_METHOD(24) {
		// A transaction's data is not written out by the server
		// until the transaction is fully closed.
		init();
		SystemTime::forceAll(YESTERDAY);
		vector<string> args;

		TransactionPtr log = core->newTransaction("foobar");
		log->message("hello world");

		TransactionPtr log2 = core2->continueTransaction(log->getTxnId(),
			log->getGroupName(), log->getCategory());
		log2->message("message 2");
		log2.reset();

		ConnectionPtr connection = core->checkoutConnection();
		writeArrayMessage(connection->fd, "flush", NULL);
		ensure(readArrayMessage(connection->fd, args));

		connection = core2->checkoutConnection();
		writeArrayMessage(connection->fd, "flush", NULL);
		ensure(readArrayMessage(connection->fd, args));

		struct stat buf;
		ensure_equals(stat(getDumpFilePath().c_str(), &buf), 0);
		ensure_equals(buf.st_size, (off_t) 0);
	}

	TEST_METHOD(29) {
		// One can supply a custom node name per openTransaction command.
		init();
		MessageClient client1 = createConnection();
		vector<string> args;

		SystemTime::forceAll(TODAY);

		client1.write("openTransaction",
			TODAY_TXN_ID, "foobar", "remote", "requests", TODAY_TIMESTAMP_STR,
			"-", "true", NULL);
		client1.write("closeTransaction", TODAY_TXN_ID, TODAY_TIMESTAMP_STR, NULL);
		client1.write("flush", NULL);
		client1.read(args);
		client1.disconnect();

		ensure(fileExists(getDumpFilePath()));
	}

	TEST_METHOD(30) {
		// A transaction is only written to the sink if it passes all given filters.
		// Test logging of new transaction.
		init();
		SystemTime::forceAll(YESTERDAY);

		TransactionPtr log = core->newTransaction("foobar", "requests", "-",
			"uri == \"/foo\""
			"\1"
			"uri != \"/bar\"");
		log->message("URI: /foo");
		log->message("transaction 1");
		log->flushToDiskAfterClose(true);
		log.reset();

		log = core->newTransaction("foobar", "requests", "-",
			"uri == \"/foo\""
			"\1"
			"uri == \"/bar\"");
		log->message("URI: /foo");
		log->message("transaction 2");
		log->flushToDiskAfterClose(true);
		log.reset();

		string data = readDumpFile();
		ensure("(1)", data.find("transaction 1\n") != string::npos);
		ensure("(2)", data.find("transaction 2\n") == string::npos);
	}

	/************************************/
}
