package ru.babobka.primecounter.task;

import ru.babobka.nodeserials.NodeRequest;
import ru.babobka.primecounter.model.PrimeCounterDistributor;
import ru.babobka.primecounter.model.PrimeCounterReducer;
import ru.babobka.primecounter.model.Range;
import ru.babobka.primecounter.runnable.MillerCountPrimesRunnable;
import ru.babobka.primecounter.util.MathUtil;
import ru.babobka.primecounter.util.ThreadUtil;
import ru.babobka.subtask.model.ExecutionResult;
import ru.babobka.subtask.model.Reducer;
import ru.babobka.subtask.model.RequestDistributor;
import ru.babobka.subtask.model.SubTask;
import ru.babobka.subtask.model.ValidationResult;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Created by dolgopolov.a on 15.12.15.
 */
public class PrimeCounterTask implements SubTask {

	private volatile AtomicReferenceArray<Thread> localThreads;

	private static final String BEGIN = "begin";

	private static final String END = "end";

	private static final String PRIME_COUNT = "primeCount";

	private static final Long MIN_RANGE_TO_PARALLEL = 5000L;

	private final PrimeCounterReducer reducer = new PrimeCounterReducer();

	private final PrimeCounterDistributor distributor = new PrimeCounterDistributor();

	private volatile boolean stopped;

	@Override
	public void stopTask() {
		stopped = true;
		ThreadUtil.interruptBatch(localThreads);
	}

	@Override
	public ExecutionResult execute(NodeRequest request) {
		if (!stopped) {
			Map<String, Serializable> result = new HashMap<>();
			int threadNum;
			if (isRequestDataTooSmall(request.getAddition())) {
				threadNum = 1;
			} else {
				threadNum = Runtime.getRuntime().availableProcessors();
			}

			result.put(PRIME_COUNT, countPrimes(((Number) request.getAddition().get(BEGIN)).longValue(),
					((Number) request.getAddition().get(END)).longValue(), threadNum));
			return new ExecutionResult(stopped, result);
		} else {
			return new ExecutionResult(stopped, null);
		}
	}

	@Override
	public ValidationResult validateRequest(NodeRequest request) {

		if (request == null) {
			return new ValidationResult("Request is empty", false);
		} else {
			try {
				long begin = ((Number) request.getAddition().get(BEGIN)).longValue();
				long end = ((Number) request.getAddition().get(END)).longValue();
				if (begin < 0 || end < 0 || begin > end) {
					return new ValidationResult("begin is more than end", false);
				}
			} catch (Exception e) {
				e.printStackTrace();
				return new ValidationResult(e.getMessage(), false);
			}
		}
		return new ValidationResult(null, true);
	}

	private int countPrimes(long rangeBegin, long rangeEnd, int threadNum) {

		AtomicIntegerArray resultArray = new AtomicIntegerArray(threadNum);
		Thread[] threads = new Thread[threadNum];
		localThreads = new AtomicReferenceArray<>(threadNum);
		int result = 0;
		Range[] ranges = MathUtil.getRangeArray(rangeBegin, rangeEnd, threadNum);

		for (int i = 0; i < ranges.length; i++) {
			localThreads.set(i, new Thread(new MillerCountPrimesRunnable(ranges[i], i, resultArray)));
			threads[i] = localThreads.get(i);
			threads[i].start();
		}

		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		for (int i = 0; i < resultArray.length(); i++) {
			result += resultArray.get(i);
		}

		return result;
	}

	@Override
	public RequestDistributor getDistributor() {
		return distributor;
	}

	@Override
	public Reducer getReducer() {
		return reducer;
	}

	@Override
	public boolean isRequestDataTooSmall(Map<String, Serializable> addition) {
		long begin = ((Number) addition.get(BEGIN)).longValue();
		long end = ((Number) addition.get(END)).longValue();
		if (end - begin > MIN_RANGE_TO_PARALLEL) {
			return false;
		}
		return true;

	}

	public SubTask newInstance() {
		return new PrimeCounterTask();
	}

}
