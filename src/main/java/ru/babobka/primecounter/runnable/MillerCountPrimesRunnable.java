package ru.babobka.primecounter.runnable;

import ru.babobka.primecounter.model.Range;
import ru.babobka.primecounter.tester.DummyPrimeTester;
import ru.babobka.primecounter.tester.PrimeTester;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Created by dolgopolov.a on 07.07.15.
 */
public class MillerCountPrimesRunnable implements Runnable {

	private final Range range;

	private final AtomicIntegerArray resultArray;

	private final int id;

	private final PrimeTester tester = new DummyPrimeTester();

	public MillerCountPrimesRunnable(Range range, int id, AtomicIntegerArray resultArray) {
		this.range = range;
		this.id = id;
		this.resultArray = resultArray;
	}

	@Override
	public void run() {
		int result = 0;
		long counter = range.getBegin();

		while (!Thread.currentThread().isInterrupted() && counter != range.getEnd() + 1) {
			if (tester.isPrime(counter)) {
				result++;
			}
			counter++;
		}

		resultArray.set(id, result);
	}

}
