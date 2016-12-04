package ru.babobka.primecounter.task;

import ru.babobka.nodeserials.NodeRequest;
import ru.babobka.primecounter.model.PrimeCounterDistributor;
import ru.babobka.primecounter.model.PrimeCounterReducer;
import ru.babobka.primecounter.model.Range;
import ru.babobka.primecounter.runnable.PrimeCounterCallable;
import ru.babobka.primecounter.util.MathUtil;

import ru.babobka.subtask.model.ExecutionResult;
import ru.babobka.subtask.model.Reducer;
import ru.babobka.subtask.model.RequestDistributor;
import ru.babobka.subtask.model.SubTask;
import ru.babobka.subtask.model.ValidationResult;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by dolgopolov.a on 15.12.15.
 */
public class PrimeCounterTask implements SubTask {

	private ExecutorService threadPool;

	private static final String BEGIN = "begin";

	private static final String END = "end";

	private static final String PRIME_COUNT = "primeCount";

	private static final Long MIN_RANGE_TO_PARALLEL = 5000L;

	private final PrimeCounterReducer reducer = new PrimeCounterReducer();

	private final PrimeCounterDistributor distributor;

	private volatile boolean stopped;

	public PrimeCounterTask() {
		distributor = new PrimeCounterDistributor("Dummy prime counter");
	}

	@Override
	public void stopTask() {
		stopped = true;
		threadPool.shutdownNow();

	}

	@Override
	public ExecutionResult execute(NodeRequest request) {
		try {
			if (!stopped) {
				Map<String, Serializable> result = new HashMap<>();
				int threadNum;
				if (isRequestDataTooSmall(request.getAddition())) {
					threadNum = 1;
				} else {
					threadNum = Runtime.getRuntime().availableProcessors();
				}
				threadPool = Executors.newFixedThreadPool(threadNum);
				try {
					result.put(PRIME_COUNT, countPrimes(((Number) request.getAddition().get(BEGIN)).longValue(),
							((Number) request.getAddition().get(END)).longValue(), threadNum));
				} catch (Exception e) {
					if (!stopped) {
						e.printStackTrace();
					}
				}
				return new ExecutionResult(stopped, result);
			} else {
				return new ExecutionResult(stopped, null);
			}
		} finally {
			if (threadPool != null) {
				threadPool.shutdownNow();
			}
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
		return new ValidationResult(true);
	}

	private int countPrimes(long rangeBegin, long rangeEnd, int parts) throws InterruptedException, ExecutionException {

		int result = 0;
		Range[] ranges = MathUtil.getRangeArray(rangeBegin, rangeEnd, parts);
		List<Future<Integer>> futureList = new ArrayList<>(ranges.length);

		for (int i = 0; i < ranges.length; i++) {
			futureList.add(threadPool.submit(new PrimeCounterCallable(ranges[i])));
		}

		for (Future<Integer> future : futureList) {
			result += future.get();
		}
		if (stopped) {
			result = 0;
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

	@Override
	public boolean isStopped() {
		return stopped;
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		PrimeCounterTask task = new PrimeCounterTask();
		System.out.println(task.countPrimes(0, 5161954, 2));
		System.out.println(task.countPrimes(5161955, 10323909, 2));
		System.out.println(task.countPrimes(10323910, 15485863, 2));
	}

}
