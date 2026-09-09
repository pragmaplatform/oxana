use crate::JOBS_PER_PAGE;

pub(crate) struct JobPage {
    pub jobs: Vec<oxana::JobEnvelope>,
    pub number: usize,
    pub total: usize,
    pub has_next: bool,
}

impl JobPage {
    pub fn list_opts(page: usize) -> oxana::QueueListOpts {
        oxana::QueueListOpts {
            count: JOBS_PER_PAGE + 1,
            offset: (page.max(1) - 1) * JOBS_PER_PAGE,
        }
    }

    pub fn new(page: usize, total: usize, mut jobs: Vec<oxana::JobEnvelope>) -> Self {
        let has_next = jobs.len() > JOBS_PER_PAGE;
        jobs.truncate(JOBS_PER_PAGE);
        Self {
            jobs,
            number: page.max(1),
            total,
            has_next,
        }
    }

    pub fn range_start(&self) -> usize {
        (self.number - 1) * JOBS_PER_PAGE + 1
    }

    pub fn range_end(&self) -> usize {
        ((self.number - 1) * JOBS_PER_PAGE + self.jobs.len()).min(self.total)
    }
}
