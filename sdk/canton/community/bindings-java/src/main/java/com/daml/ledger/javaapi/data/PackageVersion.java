package com.daml.ledger.javaapi.data;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.stream.Collectors;

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
        return Arrays.compare(this.segments, other.segments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PackageVersion that = (PackageVersion) o;
        return Arrays.equals(segments, that.segments);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(segments);
    }

    @Override
    public String toString() {
        return Arrays.stream(segments).mapToObj(Integer::toString)
                .collect(Collectors.joining("."));
    }
}
