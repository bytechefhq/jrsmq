package com.oblac.jrsmq;

import com.oblac.jrsmq.cmd.BaseQueueCmd;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;

public class Fixtures {

	public static final String TEST_QNAME = "testqueue";
	public static final String NONEXISTING_ID = "12345678901234567890123456789012";

	public static void cleanup() {
		TestRedisSMQ rsmq = Fixtures.redisSMQ();
		try {
			rsmq.deleteQueue().qname(TEST_QNAME).exec();
		}
		catch (Exception ignore) {
		}
		finally {
			rsmq.quit();
		}
	}

	/**
	 * Returns test configuration.
	 */
	public static RedisSMQConfig testConfig() {
		return new RedisSMQConfig().ns("trsmq");
	}

	public static TestRedisSMQ redisSMQ() {
		return new TestRedisSMQ(testConfig());
	}

	public static QueueDef getQueue(TestRedisSMQ redisSMQ, String name) {
		return new BaseQueueCmd<QueueDef>(redisSMQ.config(), redisSMQ::redisClient) {
			@Override
			protected QueueDef exec(RedisCommands<String, String> redisCommands) {
				return getQueue(redisCommands, name, true);
			}
		}.exec();
	}

	/**
	 * Test wrapper of RedisSMQ.
	 */
	public static class TestRedisSMQ extends RedisSMQ {
		public TestRedisSMQ(RedisSMQConfig config) {
			super(config);
		}

		public RedisSMQConfig config() {
			return config;
		}
	}
}
