/* 
 * Copyright (C) 2023-2025 Smart Software for Car Technologies Inc. and EXCEEDDATA
 *     https://www.smartsct.com
 *     https://www.exceeddata.com
 *
 *                            MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * Except as contained in this notice, the name of a copyright holder
 * shall not be used in advertising or otherwise to promote the sale, use 
 * or other dealings in this Software without prior written authorization 
 * of the copyright holder.
 */

package com.exceeddata.sdk.vdata.spark.v3;

import java.text.DecimalFormat;

import org.apache.spark.sql.sources.AlwaysFalse;
import org.apache.spark.sql.sources.AlwaysTrue;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;

public final class VDataSparkFilters {
    private VDataSparkFilters() {}
    
    public static boolean filter(final Filter filter, final String value) {
        if (filter instanceof EqualTo) {
            return equalTo((EqualTo) filter, value);
        } else if (filter instanceof Not) {
            return !filter(((Not) filter).child(), value);
        } else if (filter instanceof Or) {
            final Or orFilter = (Or) filter;
            return filter(orFilter.left(), value) || filter(orFilter.right(), value);
        } else if (filter instanceof And) {
            final And andFilter = (And) filter;
            return filter(andFilter.left(), value) && filter(andFilter.right(), value);
        } else if (filter instanceof StringStartsWith) {
            return startsWith((StringStartsWith) filter, value);
        } else if (filter instanceof StringEndsWith) {
            return endsWith((StringEndsWith) filter, value);
        } else if (filter instanceof StringContains) {
            return contains((StringContains) filter, value);
        } else if (filter instanceof In) {
            return in((In) filter, value);
        } else if (filter instanceof IsNull) {
            return isNull(value);
        } else if (filter instanceof IsNotNull) {
            return isNotNull(value);
        } else if (filter instanceof EqualNullSafe) {
            return equalNullSafe((EqualNullSafe) filter, value);
        } else if (filter instanceof GreaterThan) {
            return greaterThan((GreaterThan) filter, value);
        } else if (filter instanceof GreaterThanOrEqual) {
            return greaterThanOrEqual((GreaterThanOrEqual) filter, value);
        } else if (filter instanceof LessThan) {
            return lessThan((LessThan) filter, value);
        } else if (filter instanceof LessThanOrEqual) {
            return lessThanOrEqual((LessThanOrEqual) filter, value);
        } else if (filter instanceof AlwaysTrue) {
            return true;
        } else if (filter instanceof AlwaysFalse) {
            return false;
        }

        throw new RuntimeException("unsupported filter type: " + filter.getClass().getName());
    }
    
    public static boolean filter(final Filter filter, final Double value) {
        if (filter instanceof IsNotNull) {
            return isNotNull(value);
        } else if (filter instanceof Or) {
            final Or orFilter = (Or) filter;
            return filter(orFilter.left(), value) || filter(orFilter.right(), value);
        } else if (filter instanceof And) {
            final And andFilter = (And) filter;
            return filter(andFilter.left(), value) && filter(andFilter.right(), value);
        } else if (filter instanceof EqualTo) {
            return equalTo((EqualTo) filter, value);
        } else if (filter instanceof GreaterThan) {
            return greaterThan((GreaterThan) filter, value);
        } else if (filter instanceof GreaterThanOrEqual) {
            return greaterThanOrEqual((GreaterThanOrEqual) filter, value);
        } else if (filter instanceof LessThan) {
            return lessThan((LessThan) filter, value);
        } else if (filter instanceof LessThanOrEqual) {
            return lessThanOrEqual((LessThanOrEqual) filter, value);
        } else if (filter instanceof Not) {
            return !filter(((Not) filter).child(), value);
        } else if (filter instanceof In) {
            return in((In) filter, value);
        } else if (filter instanceof IsNull) {
            return isNull(value);
        } else if (filter instanceof EqualNullSafe) {
            return equalNullSafe((EqualNullSafe) filter, value);
        } else if (filter instanceof StringStartsWith) {
            return value != null && startsWith((StringStartsWith) filter, new DecimalFormat("0.#").format(value));
        } else if (filter instanceof StringEndsWith) {
            return value != null && endsWith((StringEndsWith) filter, new DecimalFormat("0.#").format(value));
        } else if (filter instanceof StringContains) {
            return value != null && contains((StringContains) filter, new DecimalFormat("0.#").format(value));
        } else if (filter instanceof AlwaysTrue) {
            return true;
        } else if (filter instanceof AlwaysFalse) {
            return false;
        }

        throw new RuntimeException("unsupported filter type: " + filter.getClass().getName());
    }
    
    public static boolean filter(final Filter filter, final java.sql.Timestamp value) {
        if (filter instanceof Or) {
            final Or orFilter = (Or) filter;
            return filter(orFilter.left(), value) || filter(orFilter.right(), value);
        } else if (filter instanceof And) {
            final And andFilter = (And) filter;
            return filter(andFilter.left(), value) && filter(andFilter.right(), value);
        } else if (filter instanceof GreaterThan) {
            return greaterThan((GreaterThan) filter, value);
        } else if (filter instanceof GreaterThanOrEqual) {
            return greaterThanOrEqual((GreaterThanOrEqual) filter, value);
        } else if (filter instanceof LessThan) {
            return lessThan((LessThan) filter, value);
        } else if (filter instanceof LessThanOrEqual) {
            return lessThanOrEqual((LessThanOrEqual) filter, value);
        } else if (filter instanceof EqualTo) {
            return equalTo((EqualTo) filter, value);
        } else if (filter instanceof EqualNullSafe) {
            return equalNullSafe((EqualNullSafe) filter, value);
        } else if (filter instanceof Not) {
            return !filter(((Not) filter).child(), value);
        } else if (filter instanceof In) {
            return in((In) filter, value);
        } else if (filter instanceof StringStartsWith) {
            return value != null && startsWith((StringStartsWith) filter, new DecimalFormat("0.#").format(value));
        } else if (filter instanceof StringEndsWith) {
            return value != null && endsWith((StringEndsWith) filter, new DecimalFormat("0.#").format(value));
        } else if (filter instanceof StringContains) {
            return value != null && contains((StringContains) filter, new DecimalFormat("0.#").format(value));
        } else if (filter instanceof IsNull) {
            return isNull(value);
        } else if (filter instanceof IsNotNull) {
            return isNotNull(value);
        } else if (filter instanceof AlwaysTrue) {
            return true;
        } else if (filter instanceof AlwaysFalse) {
            return false;
        }

        throw new RuntimeException("unsupported filter type: " + filter.getClass().getName());
    }
    
    public static boolean isNull(final Object value) {
        return value == null;
    }
    
    public static boolean isNotNull(final Object value) {
        return value != null;
    }
    
    public static boolean equalTo(final EqualTo filter, final String value) {
        return value != null && value.equals(filter.value().toString());
    }
    
    public static boolean equalTo(final EqualTo filter, final java.sql.Timestamp value) {
        return value != null && compareTimestamp(value, filter.value()) == 0;
    }
    
    public static boolean equalTo(final EqualTo filter, final Double value) {
        return value != null && compareDouble(value, filter.value()) == 0;
    }
    
    public static boolean equalNullSafe(final EqualNullSafe filter, final String value) {
        return value != null ? filter.value() != null && value.equals(filter.value().toString()) : filter.value() == null;
    }
    
    public static boolean equalNullSafe(final EqualNullSafe filter, final java.sql.Timestamp value) {
        return value != null ? filter.value() != null && compareTimestamp(value, filter.value()) == 0 : filter.value() == null;
    }
    
    public static boolean equalNullSafe(final EqualNullSafe filter, final Double value) {
        return value != null ? filter.value() != null && compareDouble(value, filter.value()) == 0 : filter.value() == null;
    }
    
    public static boolean startsWith(final StringStartsWith filter, final String value) {
        return value != null && value.startsWith(filter.value().toString());
    }
    
    public static boolean endsWith(final StringEndsWith filter, final String value) {
        return value != null && value.endsWith(filter.value().toString());
    }
    
    public static boolean contains(final StringContains filter, final String value) {
        return value != null && value.contains(filter.value().toString());
    }
    
    public static boolean greaterThan(final GreaterThan filter, final String value) {
        return value != null && value.compareTo(filter.value().toString()) > 0;
    }
    
    public static boolean greaterThan(final GreaterThan filter, final java.sql.Timestamp value) {
        return value != null && compareTimestamp(value, filter.value()) > 0;
    }
    
    public static boolean greaterThan(final GreaterThan filter, final Double value) {
        return value != null && compareDouble(value, filter.value()) > 0;
    }
    
    public static boolean greaterThanOrEqual(final GreaterThanOrEqual filter, final String value) {
        return value != null && value.compareTo(filter.value().toString()) >= 0;
    }
    
    public static boolean greaterThanOrEqual(final GreaterThanOrEqual filter, final java.sql.Timestamp value) {
        return value != null && compareTimestamp(value, filter.value()) >= 0;
    }
    
    public static boolean greaterThanOrEqual(final GreaterThanOrEqual filter, final Double value) {
        return value != null && compareDouble(value, filter.value()) >= 0;
    }
    
    public static boolean lessThan(final LessThan filter, final String value) {
        return value != null && value.compareTo(filter.value().toString()) < 0;
    }
    
    public static boolean lessThan(final LessThan filter, final java.sql.Timestamp value) {
        return value != null && compareTimestamp(value, filter.value()) < 0;
    }
    
    public static boolean lessThan(final LessThan filter, final Double value) {
        return value != null && compareDouble(value, filter.value()) < 0;
    }
    
    public static boolean lessThanOrEqual(final LessThanOrEqual filter, final String value) {
        return value != null && value.compareTo(filter.value().toString()) <= 0;
    }
    
    public static boolean lessThanOrEqual(final LessThanOrEqual filter, final java.sql.Timestamp value) {
        return value != null && compareTimestamp(value, filter.value()) < 0;
    }
    
    public static boolean lessThanOrEqual(final LessThanOrEqual filter, final Double value) {
        return value != null && compareDouble(value, filter.value()) <= 0;
    }
    
    public static boolean in(final In filter, final String value) {
        if (value == null) {
            return false;
        }
        for (final Object o : filter.values()) {
            if (o == null) {
                continue;
            }
            if (value.equals(o.toString())) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean in(final In filter, final Double value) {
        if (value == null) {
            return false;
        }
        for (final Object o : filter.values()) {
            if (o == null) {
                continue;
            }
            if (compareDouble(value, o) == 0) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean in(final In filter, final java.sql.Timestamp value) {
        if (value == null) {
            return false;
        }
        for (final Object o : filter.values()) {
            if (o == null) {
                continue;
            }
            if (compareTimestamp(value, o) == 0) {
                return true;
            }
        }
        return false;
    }
    
    private static int compareTimestamp(final java.sql.Timestamp value, final Object o) {
        if (o instanceof java.util.Date) {
            return value.compareTo((java.util.Date) o);
        }
        if (o instanceof Number) {
            return Long.compare(value.getTime(), ((Number) o).longValue());
        }
        throw new RuntimeException("invalid timestamp comparison type: " + o.getClass().getName());
    }
    
    private static int compareDouble(final Double value, final Object o) {
        if (o instanceof Number) {
            final double d = ((Number) o).doubleValue();
            if (value == Math.rint(value) && d == Math.rint(d)) {
                return Long.compare(value.longValue(), (long) d);
            }
            return Double.compare(value, d);
        }
        if (o instanceof String) {
            final String s = o.toString().trim();
            if (s.length() == 0) {
                return 1;
            }
            try {
                final double d = Double.parseDouble(s);
                if (value == Math.rint(value) && d == Math.rint(d)) {
                    return Long.compare(value.longValue(), (long) d);
                }
                return Double.compare(value, d);
            } catch (NumberFormatException e) {
                throw new RuntimeException("not parsable number: " + s);
            }
        }
        throw new RuntimeException("invalid double comparison type: " + o.getClass().getName());
    }
    
    public static boolean filterWithinTimeRange(final Filter filter, final java.sql.Timestamp storageStartTime, final java.sql.Timestamp storageEndTime) {
        if (filter instanceof Or) {
            final Or orFilter = (Or) filter;
            return filterWithinTimeRange(orFilter.left(), storageStartTime, storageEndTime) || filterWithinTimeRange(orFilter.right(), storageStartTime, storageEndTime);
        } else if (filter instanceof And) {
            final And andFilter = (And) filter;
            return filterWithinTimeRange(andFilter.left(), storageStartTime, storageEndTime) && filterWithinTimeRange(andFilter.right(), storageStartTime, storageEndTime);
        } else if (filter instanceof GreaterThan) {
            return greaterThanWithinTimeRange((GreaterThan) filter, storageStartTime, storageEndTime);
        } else if (filter instanceof GreaterThanOrEqual) {
            return greaterThanOrEqualWithinTimeRange((GreaterThanOrEqual) filter, storageStartTime, storageEndTime);
        } else if (filter instanceof LessThan) {
            return lessThanWithinTimeRange((LessThan) filter, storageStartTime, storageEndTime);
        } else if (filter instanceof LessThanOrEqual) {
            return lessThanOrEqualWithinTimeRange((LessThanOrEqual) filter, storageStartTime, storageEndTime);
        } else if (filter instanceof EqualTo) {
            return equalToWithinTimeRange((EqualTo) filter, storageStartTime, storageEndTime);
        } else if (filter instanceof EqualNullSafe) {
            return equalNullSafeWithinTimeRange((EqualNullSafe) filter, storageStartTime, storageEndTime);
        } else if (filter instanceof Not) {
            return !filterWithinTimeRange(((Not) filter).child(), storageStartTime, storageEndTime);
        } else if (filter instanceof In) {
            return inWithinTimeRange((In) filter, storageStartTime, storageEndTime);
        } else if (filter instanceof StringStartsWith || filter instanceof StringEndsWith || filter instanceof StringContains) {
            return true; //not possible to know
        } else if (filter instanceof IsNull) {
            return false;
        } else if (filter instanceof IsNotNull) {
            return true;
        }

        throw new RuntimeException("unsupported filter type: " + filter.getClass().getName());
    }
    
    public static boolean greaterThanWithinTimeRange(final GreaterThan filter, final java.sql.Timestamp storageStartTime, final java.sql.Timestamp storageEndTime) {
        return !(compareTimestamp(storageEndTime, filter.value()) <= 0);
    }
    
    public static boolean greaterThanOrEqualWithinTimeRange(final GreaterThanOrEqual filter, final java.sql.Timestamp storageStartTime, final java.sql.Timestamp storageEndTime) {
        return !(compareTimestamp(storageEndTime, filter.value()) <= 0);
    }
    
    public static boolean lessThanWithinTimeRange(final LessThan filter, final java.sql.Timestamp storageStartTime, final java.sql.Timestamp storageEndTime) {
        return !(compareTimestamp(storageStartTime, filter.value()) > 0);
    }
    
    public static boolean lessThanOrEqualWithinTimeRange(final LessThanOrEqual filter, final java.sql.Timestamp storageStartTime, final java.sql.Timestamp storageEndTime) {
        return !(compareTimestamp(storageStartTime, filter.value()) >= 0);
    }
    
    public static boolean equalToWithinTimeRange(final EqualTo filter, final java.sql.Timestamp storageStartTime, final java.sql.Timestamp storageEndTime) {
        return compareTimestamp(storageStartTime, filter.value()) <= 0 && compareTimestamp(storageEndTime, filter.value()) >= 0;
    }
    
    public static boolean equalNullSafeWithinTimeRange(final EqualNullSafe filter, final java.sql.Timestamp storageStartTime, final java.sql.Timestamp storageEndTime) {
        return compareTimestamp(storageStartTime, filter.value()) <= 0 && compareTimestamp(storageEndTime, filter.value()) >= 0;
    }
    
    public static boolean inWithinTimeRange(final In filter, final java.sql.Timestamp storageStartTime, final java.sql.Timestamp storageEndTime) {
        for (final Object o : filter.values()) {
            if (o == null) {
                continue;
            }
            if (compareTimestamp(storageStartTime, o) <= 0 && compareTimestamp(storageEndTime, o) >= 0) {
                return true; //at least one in list within the range,
            }
        }
        return false;
    }
}
