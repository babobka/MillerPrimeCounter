package ru.babobka.primecounter.task;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import ru.babobka.nodeserials.NodeRequest;
import ru.babobka.primecounter.task.PrimeCounterTask;
import ru.babobka.subtask.model.SubTask;

public class TaskTest {

	private final SubTask TASK = new PrimeCounterTask();

	private NodeRequest tenPrimesRequest;

	private NodeRequest thousandPrimesRequest;

	private NodeRequest tenThousandPrimesRequest;

	private NodeRequest millionPrimesRequest;

	@Before
	public void init() {
		Map<String, Serializable> additionMap = new HashMap<>();
		additionMap.put("begin", 0);
		additionMap.put("end", 15_485_863);
		millionPrimesRequest = new NodeRequest(1, 1, "millerPrimeCounter", additionMap, false, false);

		additionMap = new HashMap<>();
		additionMap.put("begin", 0);
		additionMap.put("end", 7919);
		thousandPrimesRequest = new NodeRequest(1, 1, "millerPrimeCounter", additionMap, false, false);

		additionMap = new HashMap<>();
		additionMap.put("begin", 0);
		additionMap.put("end", 104729);
		tenThousandPrimesRequest = new NodeRequest(1, 1, "millerPrimeCounter", additionMap, false, false);

		additionMap = new HashMap<>();
		additionMap.put("begin", 0);
		additionMap.put("end", 29);
		tenPrimesRequest = new NodeRequest(1, 1, "millerPrimeCounter", additionMap, false, false);
	}

	@Test
	public void testValidation() {
		assertTrue(TASK.validateRequest(thousandPrimesRequest).isValid());
		thousandPrimesRequest.getAddition().put("begin", -1);
		assertFalse(TASK.validateRequest(thousandPrimesRequest).isValid());
	}

	
	@Test
	public void testMillionPrimes() {
		assertEquals(TASK.execute(millionPrimesRequest).getResultMap().get("primeCount"), 1_000_000);
	}
	
	@Test
	public void testTenThousandPrimes() {
		assertEquals(TASK.execute(tenThousandPrimesRequest).getResultMap().get("primeCount"), 10000);
	}

	@Test
	public void testTenPrimes() {
		assertEquals(TASK.execute(tenPrimesRequest).getResultMap().get("primeCount"), 10);
	}

	@Test
	public void testThousandPrimes() {
		assertEquals(TASK.execute(thousandPrimesRequest).getResultMap().get("primeCount"), 1000);
	}

}
