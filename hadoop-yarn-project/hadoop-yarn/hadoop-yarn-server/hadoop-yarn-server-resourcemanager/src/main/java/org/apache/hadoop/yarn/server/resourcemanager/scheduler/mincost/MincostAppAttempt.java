package org.apache.hadoop.yarn.server.resourcemanager.scheduler.mincost;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoAppAttempt;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

public class MincostAppAttempt extends FiCaSchedulerApp {
    private static final Log LOG = LogFactory.getLog(FifoAppAttempt.class);
    // smartnews change FifoAppAttempt contructor to be public
    public MincostAppAttempt(ApplicationAttemptId appAttemptId, String user,
                          Queue queue, ActiveUsersManager activeUsersManager,
                          RMContext rmContext) {
        super(appAttemptId, user, queue, activeUsersManager, rmContext);
    }

    public RMContainer allocate(NodeType type, FiCaSchedulerNode node,
                                SchedulerRequestKey schedulerKey, Container container) {
        try {
            writeLock.lock();

            if (isStopped) {
                return null;
            }

            // Required sanity check - AM can call 'allocate' to update resource
            // request without locking the scheduler, hence we need to check
            if (getOutstandingAsksCount(schedulerKey) <= 0) {
                return null;
            }

            // Create RMContainer
            RMContainer rmContainer = new RMContainerImpl(container,
                    schedulerKey, this.getApplicationAttemptId(), node.getNodeID(),
                    appSchedulingInfo.getUser(), this.rmContext, node.getPartition());
            ((RMContainerImpl) rmContainer).setQueueName(this.getQueueName());

            updateAMContainerDiagnostics(AMState.ASSIGNED, null);

            // Add it to allContainers list.
            addToNewlyAllocatedContainers(node, rmContainer);

            ContainerId containerId = container.getId();
            liveContainers.put(containerId, rmContainer);

            // Update consumption and track allocations
            ContainerRequest containerRequest = appSchedulingInfo.allocate(
                    type, node, schedulerKey, container);

            attemptResourceUsage.incUsed(node.getPartition(),
                    container.getResource());

            // Update resource requests related to "request" and store in RMContainer
            ((RMContainerImpl) rmContainer).setContainerRequest(containerRequest);

            // Inform the container
            rmContainer.handle(
                    new RMContainerEvent(containerId, RMContainerEventType.START));

            if (LOG.isDebugEnabled()) {
                LOG.debug("allocate: applicationAttemptId=" + containerId
                        .getApplicationAttemptId() + " container=" + containerId + " host="
                        + container.getNodeId().getHost() + " type=" + type);
            }
            // In order to save space in the audit log, only include the partition
            // if it is not the default partition.
            String partition = null;
            if (appAMNodePartitionName != null &&
                    !appAMNodePartitionName.isEmpty()) {
                partition = appAMNodePartitionName;
            }
            RMAuditLogger.logSuccess(getUser(),
                    RMAuditLogger.AuditConstants.ALLOC_CONTAINER, "SchedulerApp",
                    getApplicationId(), containerId, container.getResource(),
                    getQueueName(), partition);

            return rmContainer;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public ApplicationResourceUsageReport getResourceUsageReport() {
        try {
            // Use write lock here because
            // SchedulerApplicationAttempt#getResourceUsageReport updated fields
            // TODO: improve this
            writeLock.lock();
            ApplicationResourceUsageReport report = super.getResourceUsageReport();
            Resource cluster = rmContext.getScheduler().getClusterResource();
            Resource totalPartitionRes =
                    rmContext.getNodeLabelManager().getResourceByLabel(
                            getAppAMNodePartitionName(), cluster);
            ResourceCalculator calc =
                    rmContext.getScheduler().getResourceCalculator();
            if (!calc.isInvalidDivisor(totalPartitionRes)) {
                LOG.warn("isInvalidDivisor returned false, lable=" + getAppAMNodePartitionName()
                        + ", resource:" + totalPartitionRes.toString());
            }
            return report;
        } finally {
            writeLock.unlock();
        }
    }
}
