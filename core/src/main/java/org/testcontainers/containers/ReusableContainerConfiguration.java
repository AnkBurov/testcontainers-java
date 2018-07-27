package org.testcontainers.containers;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang.StringUtils;

import static org.testcontainers.containers.ConflictingImageVersionsReuseBehaviour.FAIL;

/**
 * @author Eugeny Karpov
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ReusableContainerConfiguration {

    private final String containerName;
    private final ConflictingImageVersionsReuseBehaviour imageConflictBehaviour;
    private final boolean isEnabled;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String containerName;
        private Boolean isEnabled = true;
        private ConflictingImageVersionsReuseBehaviour imageConflictBehaviour = FAIL;

        public Builder withContainerName(String containerName) {
            this.containerName = containerName;
            return this;
        }

        public Builder withConflictingImageVersionsReuseBehaviour(ConflictingImageVersionsReuseBehaviour behaviour) {
            this.imageConflictBehaviour = behaviour;
            return this;
        }

        public Builder isEnabled(boolean isEnabled) {
            this.isEnabled = isEnabled;
            return this;
        }

        public ReusableContainerConfiguration build() {
            if (StringUtils.isBlank(containerName)) {
                throw new IllegalArgumentException("Container name must be specified with REUSABLE mode");
            }
            if (imageConflictBehaviour == null) { //just in case
                throw new IllegalArgumentException("ConflictingImageVersionsReuseBehaviour cannot be null");
            }
            return new ReusableContainerConfiguration(containerName, imageConflictBehaviour, isEnabled);
        }
    }
}
