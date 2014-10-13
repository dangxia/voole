package com.voole.hobbit2.camus.hive.order.mixed;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.voole.hobbit2.camus.hive.order.mixed.jobcontrol.ControlledJob;
import com.voole.hobbit2.camus.hive.order.mixed.jobcontrol.JobControl;
import com.voole.hobbit2.camus.mr.CamusJobCreator;
import com.voole.hobbit2.hive.order.HiveOrderJobCreator;

public class MixedJobChain extends Configured implements Tool {
	private final JobControl jobControl;

	public MixedJobChain() {
		this("camus-hive-order-mixed");
	}

	public MixedJobChain(String jobname) {
		jobControl = new JobControl(jobname);
	}

	@Override
	public int run(String[] args) throws Exception {
		fillJobControl(args);
		new Thread(jobControl).start();
		return waitJobsComplete();
	}

	private int waitJobsComplete() throws Exception {
		try {
			while (!jobControl.allFinished()) {
				if (jobControl.getFailedJobList().size() > 0) {
					printJobsState();
					return 1;
				}
				checkRunningJobs();
				TimeUnit.SECONDS.sleep(5);
			}
			printJobsState();
			if (jobControl.getFailedJobList().size() > 0) {
				return 1;
			}
			return 0;
		} catch (Exception e) {
			throw e;
		} finally {
			jobControl.stop();
		}
	}

	private Set<String> waitingJobs = new HashSet<String>();

	private void checkRunningJobs() {
		List<ControlledJob> jobs = jobControl.getRunningJobList();
		for (ControlledJob job : jobs) {
			if (!waitingJobs.contains(job.getJobName())) {
				waitForCompletion(job);
			}
		}

	}

	private void printJobsState() {
		List<ControlledJob> jobs = jobControl.getReadyJobsList();
		System.out.print("-------ready jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {
			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");

		jobs = jobControl.getWaitingJobList();
		System.out.print("-------waiting jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {
			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");

		jobs = jobControl.getRunningJobList();
		System.out.print("-------running jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {
			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");

		jobs = jobControl.getSuccessfulJobList();
		System.out.print("-------Successed jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {

			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");

		jobs = jobControl.getFailedJobList();
		System.out.print("-------Failed jobs size:" + jobs.size() + ",");
		for (ControlledJob job : jobs) {
			System.out.print(job.getJobName() + "\t");
		}
		System.out.println("");
	}

	protected void waitForCompletion(final ControlledJob job) {
		waitingJobs.add(job.getJobName());
		new Thread() {
			public void run() {
				try {
					job.getJob().waitForCompletion(true);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			};
		}.start();
	}

	protected void fillJobControl(String[] args) throws Exception {
		ControlledJob camusControlledJob = createCamusControlledJob(args);
		ControlledJob hiveOrderControlledJob = createHiveOrderControlledJob(args);
		hiveOrderControlledJob.addDependingJob(camusControlledJob);

		jobControl.addJob(camusControlledJob);
		jobControl.addJob(hiveOrderControlledJob);

	}

	protected ControlledJob createHiveOrderControlledJob(String[] args)
			throws IOException, ConfigurationException, ParseException {
		HiveOrderJobCreator hiveOrderJobCreator = new HiveOrderJobCreator();
		hiveOrderJobCreator.setConf(getConf());
		Job hiveOrderJob = hiveOrderJobCreator.create(args);
		ControlledJob hiveOrderControlledJob = new ControlledJob(
				hiveOrderJob.getConfiguration());
		hiveOrderControlledJob.setJob(hiveOrderJob);

		return hiveOrderControlledJob;
	}

	protected ControlledJob createCamusControlledJob(String[] args)
			throws IOException, ConfigurationException, ParseException {
		CamusJobCreator camusJobCreator = new CamusJobCreator();
		camusJobCreator.setConf(getConf());
		Job camusJob = camusJobCreator.create(args);

		ControlledJob camusControlledJob = new ControlledJob(
				camusJob.getConfiguration());
		camusControlledJob.setJob(camusJob);

		return camusControlledJob;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MixedJobChain(), args));
	}

	public JobControl getJobControl() {
		return jobControl;
	}

}
