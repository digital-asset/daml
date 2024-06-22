package com.daml.ledger.javaapi.data;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public class PackageVersion implements Comparable<PackageVersion> {
    private final int[] segments;

    /**
     * Creates a PackageVersion from the provided segments.
     * <p>
     * This method is meant only for internal API usage.
     * It is marked unsafe as it does not validate the input
     * according to the accepted ledger format of PackageVersion.
     */
    public PackageVersion(int[] segments) {
        this.segments = segments;
    }

    /**
     * Parses the provided String value into a PackageVersion.
     * <p>
     * This method is meant only for internal API usage.
     * It is marked unsafe as it does not validate the input
     * according to the accepted ledger format of PackageVersion.
     */
    public static PackageVersion unsafeFromString(@NonNull String version) {
        String[] parts = version.split("\\.");
        int[] segments = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            segments[i] = Integer.parseInt(parts[i]);
            if (segments[i] < 0) {
                throw new IllegalArgumentException("Invalid version. No negative segments allowed: " + version);
            }
        }
        return new PackageVersion(segments);
    }

    @Override
    public int compareTo(PackageVersion other) {
        int minLength = Math.min(this.segments.length, other.segments.length);
        for (int i = 0; i < minLength; i++) {
            if (this.segments[i] != other.segments[i]) {
                return this.segments[i] - other.segments[i];
            }
        }
        return this.segments.length - other.segments.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PackageVersion that = (PackageVersion) o;
        return Objects.deepEquals(segments, that.segments);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(segments);
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(".");
        for (int segment : segments) {
            sj.add(Integer.toString(segment));
        }
        return sj.toString();
    }
}
