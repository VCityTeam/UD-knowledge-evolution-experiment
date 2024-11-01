
CREATE TABLE Experience (
  PRIMARY KEY (id_experience),
  id_experience   integer NOT NULL,
  exp_description text
);

CREATE TABLE has_OSLabel (
  PRIMARY KEY (id_metric_os, key_label),
  id_metric_os integer NOT NULL,
  key_label    varchar(255) NOT NULL
);

CREATE TABLE has_Pr_Label (
  PRIMARY KEY (id_metric_pr, key_label),
  id_metric_pr integer NOT NULL,
  key_label    varchar(255) NOT NULL
);

CREATE TABLE Job (
  PRIMARY KEY (id_job),
  id_job integer NOT NULL,
  id_pod integer NOT NULL,
  UNIQUE (id_pod)
);

CREATE TABLE OSLabel (
  PRIMARY KEY (key_label),
  key_label   varchar(255) NOT NULL,
  label_value varchar(255)
);

CREATE TABLE OSLog (
  PRIMARY KEY (id_os_log),
  id_os_log    integer NOT NULL,
  log_line     varchar(255),
  log_time     timestamp,
  id_metric_os integer NOT NULL
);

CREATE TABLE OSMetric (
  PRIMARY KEY (id_metric_os),
  id_metric_os integer NOT NULL,
  bd_name      varchar(255),
  index_name   varchar(255),
  id_pod       integer NOT NULL
);

CREATE TABLE Pod (
  PRIMARY KEY (id_pod),
  id_pod        integer NOT NULL,
  id_task_model integer NULL,
  id_wf_run     integer NULL
);

CREATE TABLE Pr_Label (
  PRIMARY KEY (key_label),
  key_label   varchar(255) NOT NULL,
  label_value varchar(255)
);

CREATE TABLE Pr_Measure (
  PRIMARY KEY (id_pr_measure),
  id_pr_measure integer NOT NULL,
  measure       varchar(255),
  measure_time  timestamp,
  id_metric_pr  integer NOT NULL
);

CREATE TABLE Pr_Metric (
  PRIMARY KEY (id_metric_pr),
  id_metric_pr integer NOT NULL,
  bd_name      varchar(255),
  mesure_name  varchar(255),
  id_pod       integer NOT NULL
);

CREATE TABLE Task_Model (
  PRIMARY KEY (id_task_model),
  id_task_model integer NOT NULL,
  id_experience integer NOT NULL
);

CREATE TABLE Workflow_Run (
  PRIMARY KEY (id_wf_run),
  id_wf_run     integer NOT NULL,
  id_experience integer NOT NULL
);

ALTER TABLE has_OSLabel ADD FOREIGN KEY (key_label) REFERENCES OSLabel (key_label);
ALTER TABLE has_OSLabel ADD FOREIGN KEY (id_metric_os) REFERENCES OSMetric (id_metric_os);

ALTER TABLE has_Pr_Label ADD FOREIGN KEY (key_label) REFERENCES Pr_Label (key_label);
ALTER TABLE has_Pr_Label ADD FOREIGN KEY (id_metric_pr) REFERENCES Pr_Metric (id_metric_pr);

ALTER TABLE Job ADD FOREIGN KEY (id_pod) REFERENCES Pod (id_pod);

ALTER TABLE OSLog ADD FOREIGN KEY (id_metric_os) REFERENCES OSMetric (id_metric_os);

ALTER TABLE OSMetric ADD FOREIGN KEY (id_pod) REFERENCES Pod (id_pod);

ALTER TABLE Pod ADD FOREIGN KEY (id_wf_run) REFERENCES Workflow_Run (id_wf_run);
ALTER TABLE Pod ADD FOREIGN KEY (id_task_model) REFERENCES Task_Model (id_task_model);

ALTER TABLE Pr_Measure ADD FOREIGN KEY (id_metric_pr) REFERENCES Pr_Metric (id_metric_pr);

ALTER TABLE Pr_Metric ADD FOREIGN KEY (id_pod) REFERENCES Pod (id_pod);

ALTER TABLE Task_Model ADD FOREIGN KEY (id_experience) REFERENCES Experience (id_experience);

ALTER TABLE Workflow_Run ADD FOREIGN KEY (id_experience) REFERENCES Experience (id_experience);