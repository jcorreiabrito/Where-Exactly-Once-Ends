package com.wxeo.failpoint;

/** The fixed Level-1 failpoint names (docs/CONTRACTS.md §7, docs/method.md Appendix B). */
public final class FailpointNames {

    public static final String AFTER_FETCH = "after_fetch";
    public static final String BEFORE_EFFECT_COMMIT = "before_effect_commit";
    public static final String AFTER_EFFECT_COMMIT = "after_effect_commit";
    public static final String BEFORE_PROGRESS_COMMIT = "before_progress_commit";
    public static final String AFTER_PROGRESS_COMMIT = "after_progress_commit";
    public static final String BEFORE_PUBLISH = "before_publish";
    public static final String BEFORE_RELAY_MARK = "before_relay_mark";

    public static final java.util.List<String> ALL = java.util.List.of(
            AFTER_FETCH, BEFORE_EFFECT_COMMIT, AFTER_EFFECT_COMMIT,
            BEFORE_PROGRESS_COMMIT, AFTER_PROGRESS_COMMIT, BEFORE_PUBLISH, BEFORE_RELAY_MARK);

    private FailpointNames() {
    }

    public static boolean isKnown(String name) {
        return ALL.contains(name);
    }
}
