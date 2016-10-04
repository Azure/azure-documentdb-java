package com.microsoft.azure.documentdb.internal.query;

import com.microsoft.azure.documentdb.Undefined;

final class OrderByItemTypeHelper {
	public static OrderByItemType getOrderByItemType(Object obj) {
		if (obj == null) {
			throw new IllegalArgumentException("obj");
		}

		if (obj instanceof Undefined) {
			return OrderByItemType.NoValue;
		}

		if (obj.equals(null)) {
			return OrderByItemType.Null;
		}

		if (obj instanceof Boolean) {
			return OrderByItemType.Boolean;
		}

		if (obj instanceof Number) {
			return OrderByItemType.Number;
		}

		if (obj instanceof String) {
			return OrderByItemType.String;
		}

        throw new IllegalArgumentException(String.format("Unexpected type: %s", obj.getClass().toString()));
	}
}