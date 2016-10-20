package ru.babobka.primecounter.util;

import ru.babobka.primecounter.model.Range;

/**
 * Created by dolgopolov.a on 06.07.15.
 */
public interface MathUtil {

	public static int log(long n) {
		return (int) Math.ceil(Math.log(n) / Math.log(2));
	}

	public static Range[] getRangeArray(long begin, long end, int parts) {
		Range[] ranges = new Range[parts];
		long portion = (end - begin) / parts;
		Range tempRange;
		for (int i = 0; i < parts; i++) {
			tempRange = new Range();
			tempRange.setBegin(begin);
			begin += portion;
			if (i == parts - 1) {
				tempRange.setEnd(end);
			} else {
				tempRange.setEnd(begin);
				begin++;
			}
			ranges[i] = tempRange;
		}

		return ranges;
	}

	public static long gcd(long a, long b) {
		if (b == 0) {
			return a;
		} else {
			return gcd(b, a % b);
		}
	}



}
