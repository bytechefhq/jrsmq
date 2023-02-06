package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.Fixtures;
import com.oblac.jrsmq.QueueMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.oblac.jrsmq.Fixtures.TEST_QNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ChangeMessageVisibilityTest {

	@BeforeEach
	public void setUp() {
		Fixtures.cleanup();
	}

	@Test
	public void testChangeMessageVisibility_noMessage() {
		Fixtures.TestRedisSMQ rsmq = Fixtures.redisSMQ();

		rsmq.createQueue().qname(TEST_QNAME).exec();

		int result = rsmq.changeMessageVisibility().qname(TEST_QNAME).id(Fixtures.NONEXISTING_ID).exec();

		assertEquals(0, result);

		// clean up

		rsmq.deleteQueue().qname(TEST_QNAME).exec();
		rsmq.quit();
	}

	@Test
	public void testChangeMessageVisibility() {
		Fixtures.TestRedisSMQ rsmq = Fixtures.redisSMQ();

		rsmq.createQueue().qname(TEST_QNAME).exec();

		String id = rsmq.sendMessage().qname(TEST_QNAME).message("Hello World").exec();

		int result = rsmq.changeMessageVisibility().qname(TEST_QNAME).id(id).vt(10000).exec();

		assertEquals(1, result);

		QueueMessage queueMessage = rsmq.receiveMessage().qname(TEST_QNAME).exec();

		assertNull(queueMessage);	// message is not visible anymore.

		// clean up

		rsmq.deleteQueue().qname(TEST_QNAME).exec();
		rsmq.quit();
	}
}
