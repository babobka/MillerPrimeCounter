package ru.babobka.miller.model;

import ru.babobka.nodeserials.NodeResponse;
import ru.babobka.subtask.exception.ReducingException;
import ru.babobka.subtask.model.Reducer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dolgopolov.a on 03.08.15.
 */
public final class PrimeCounterReducer implements Reducer {

	@Override
	public Map<String, Serializable> reduce(
			List<NodeResponse> responses) throws ReducingException {
		try {
			Long result = 0L;
			for (NodeResponse response : responses) {
				if (isValidResponse(response)) {
					result += (Integer) response.getAddition()
							.get("primeCount");
				} else {
					throw new ReducingException();
				}
			}
			Map<String, Serializable> resultMap = new HashMap<>();
			resultMap.put("primeCount", result);
			return resultMap;
		} catch (Exception e) {
			e.printStackTrace();
		}
		throw new ReducingException();
	}

	@Override
	public boolean isValidResponse(NodeResponse response) {
		if (response!=null && response.getStatus() == NodeResponse.Status.NORMAL
				&& response.getAddition().get("primeCount") != null) {
			return true;
		}
		return false;
	}

}
