import sqlite3
from datetime import date
from pprint import pprint
from typing import Any, Iterable, Optional, Set

from git import Commit, Repo
from yaml import safe_load


class Job:
    title: str
    link: Optional[str]
    indeed: Optional[str]
    remote: Optional[bool]
    company: str

    date_posted: date
    date_removed: Optional[date]

    def __init__(
        self,
        title: str,
        company: str,
        date_posted: date,
        link: Optional[str] = None,
        indeed: Optional[str] = None,
        remote: Optional[bool] = None,
        indeeed: Optional[str] = None,
    ):
        self.title = title
        self.link = link
        self.indeed = indeed or indeeed
        self.remote = remote
        self.company = company
        self.date_posted = date_posted
        self.date_removed = None

    def __dict_repr__(self) -> dict:
        return {
            "title": self.title,
            # "link": self.link,
            # "indeed": self.indeed,
            "remote": self.remote,
            "company": self.company,
        }

    def __hash__(self) -> int:
        return hash(str(self.__dict_repr__()))

    def __eq__(self, other) -> bool:
        return self.__dict_repr__() == other.__dict_repr__()

    def __repr__(self) -> str:
        return f"Job(title={self.title}, company={self.company}, date_posted={self.date_posted}, date_removed={self.date_removed}, link={self.link}, indeed={self.indeed}, remote={self.remote})"


def get_equivalent_job(job: Job, job_set: Iterable[Job]) -> Job:
    return next(job_obj for job_obj in job_set if job_obj == job)


def parse_job_postings(
    job_data: Any, previous_jobs: Set[Job], commit: Commit
) -> Set[Job]:
    jobs = set()

    for company in job_data:
        if company["company"] == "whalecompany":
            company["company"] = "heyorca"
        for posting in company["jobs"]:
            if "post_date" in posting:
                for job in posting["jobs"]:
                    jobs.add(
                        Job(
                            **job,
                            date_posted=posting["post_date"],
                            company=company["company"],
                        )
                    )
            else:
                job_obj = Job(**posting, date_posted=None, company=company["company"])
                if job_obj not in previous_jobs:
                    job_obj.date_posted = commit.authored_datetime.date()
                else:
                    print("Job in last jobs, re-using post date")
                    job_obj.date_posted = get_equivalent_job(
                        job_obj, previous_jobs
                    ).date_posted
                jobs.add(job_obj)

    return jobs


REPO_PATH = "./CTS-NL.github.io"
JOBS_PATH = "_data/jobs.yml"

cts_repo = Repo("./CTS-NL.github.io")

all_jobs = []

previous_jobs = set()

for commit in cts_repo.iter_commits(paths=(JOBS_PATH,), reverse=True):
    jobs_dict = safe_load((commit.tree / JOBS_PATH).data_stream)
    new_jobs = parse_job_postings(jobs_dict, previous_jobs, commit)

    for job in previous_jobs:
        if job not in new_jobs:
            print(f"Job {job} removed")
            get_equivalent_job(
                job, all_jobs[::-1]
            ).date_removed = commit.authored_datetime.date()
    for job in new_jobs:
        if job not in previous_jobs:
            print(f"Job {job} added")
            all_jobs.append(job)

    previous_jobs = new_jobs

print(f"Successfully parsed {len(all_jobs)} jobs, dumping to jobs.sqlite")

con = sqlite3.connect("jobs.sqlite")
cur = con.cursor()

cur.execute("DROP TABLE IF EXISTS jobs")
cur.execute(
    """
    CREATE TABLE jobs (
        title TEXT,
        link TEXT,
        indeed TEXT,
        remote BOOLEAN,
        company TEXT,
        date_posted DATE,
        date_removed DATE
    )
    """
)

for job in all_jobs:
    cur.execute(
        """
        INSERT INTO jobs (
            title,
            link,
            indeed,
            remote,
            company,
            date_posted,
            date_removed
        ) VALUES (
            :title,
            :link,
            :indeed,
            :remote,
            :company,
            :date_posted,
            :date_removed
        )
        """,
        job.__dict__,
    )

con.commit()
con.close()
