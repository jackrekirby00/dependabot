import requests
import pandas as pd
import duckdb
import typing as t
import os
from dotenv import load_dotenv


def get_dependabot_alerts_from_repository(token: str, org: str, repo: str):
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    page = 1
    alerts = []

    while True:
        response = requests.get(
            f"https://api.github.com/repos/{org}/{repo}/dependabot/alerts?page={page}&per_page=100",
            headers=headers,
        )

        if response.status_code == 200:
            page_alerts = response.json()
            if not page_alerts:
                break
            for alert in page_alerts:
                alert["repository"] = repo
            alerts.extend(page_alerts)
            page += 1
        else:
            raise Exception(
                f"API call failed with status code {response.status_code}, {response.reason}"
            )

    return alerts


def list_repositories_in_organisation(org: str, token: str):
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    page = 1
    repos = []

    while True:
        url = f"https://api.github.com/orgs/{org}/repos?page={page}&per_page=100"
        response = requests.get(
            url,
            headers=headers,
        )

        if response.status_code == 200:
            page_repos = response.json()
            if not page_repos:
                break
            repos.extend([repo["name"] for repo in page_repos if not repo["archived"]])
            page += 1
        else:
            raise Exception(
                f"API call failed with status code {response.status_code}, {response.reason}."
                f" {url} {headers}"
            )

    return repos


def generate_raw_alerts_table(github_organisation: str, github_token: str) -> None:
    # skip if raw alerts file already exists
    if os.path.exists("outputs/raw_alerts.csv"):
        print("Raw alerts file already exists, skipping...")
        return

    print("Generating raw alerts table...")
    repositories: t.List[str] = list_repositories_in_organisation(
        github_organisation, github_token
    )
    print(f"Found {len(repositories)} repositories")

    alerts: t.List = []

    for i, repo in enumerate(repositories):
        try:
            alerts.extend(
                get_dependabot_alerts_from_repository(
                    token=github_token,
                    org=github_organisation,
                    repo=repo,
                )
            )
            print(f" > {i}/{len(repositories)} : Fetched alerts for {repo}")
        except Exception as e:
            print(f" > {i}/{len(repositories)} : Error fetching alerts for {repo}: {e}")

    df = pd.json_normalize(alerts)

    df.to_csv("outputs/raw_alerts.csv", index=False)


def generate_processed_alert_tables() -> None:
    print("Generating processed alert tables...")
    df = pd.read_csv("outputs/raw_alerts.csv")

    df = df[
        [
            "repository",
            "number",
            "state",
            "created_at",
            "fixed_at",
            "dismissed_at",
            "dismissed_by",
            "dismissed_reason",
            "dismissed_comment",
            "security_advisory.summary",
            "security_advisory.ghsa_id",
            "security_advisory.severity",
            "url",
        ]
    ]

    df.rename(
        columns={
            "security_advisory.summary": "summary",
            "security_advisory.ghsa_id": "ghsa_id",
            "security_advisory.severity": "severity",
        },
        inplace=True,
    )

    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            repository VARCHAR, number INTEGER, state VARCHAR,
            created_at TIMESTAMP, fixed_at TIMESTAMP,
            dismissed_at TIMESTAMP, dismissed_by VARCHAR,
            dismissed_reason VARCHAR, dismissed_comment VARCHAR,
            summary VARCHAR, ghsa_id VARCHAR, severity VARCHAR,
            url VARCHAR
        )"""
    )
    con.execute("INSERT INTO alerts SELECT * FROM df")

    con.execute("UPDATE alerts SET state = 'dismissed' WHERE dismissed_at IS NOT NULL")

    # delete rows where severity is not critical
    con.execute("DELETE FROM alerts WHERE severity != 'critical'")

    # delete severity column
    con.execute("ALTER TABLE alerts DROP COLUMN severity")

    con.execute("COPY (SELECT * FROM alerts) TO 'outputs/critical_alerts.csv' (HEADER)")

    con.execute(
        """
        CREATE TABLE critical_alert_count AS
        SELECT *
        FROM (
            SELECT repository, state, COUNT(*) as count
            FROM alerts
            GROUP BY repository, state
        ) src
        PIVOT (
            SUM(count)
            FOR state IN ('open', 'dismissed', 'fixed')
        )
        ORDER BY open DESC
        """
    )

    for state in ["open", "dismissed", "fixed"]:
        con.execute(
            f"""
            COPY (select repository,
                COALESCE({state}, 0) as {state}
                from critical_alert_count
                where {state} > 0
                ORDER BY {state} DESC
            ) TO 'outputs/{state}_critical_alert_count.csv' (HEADER)
            """
        )

    con.execute(
        """
        COPY (select repository,
            COALESCE(open, 0) as open,
            COALESCE(dismissed, 0) as dismissed,
            COALESCE(fixed, 0) as fixed
            from critical_alert_count
            ORDER BY open DESC
        ) TO 'outputs/all_critical_alert_count.csv' (HEADER)
        """
    )

    con.execute(
        """
        COPY (
            select max(created_at) as created_at, ghsa_id
            from alerts
            where state = 'open'
            group by ghsa_id
            ORDER BY created_at DESC
        ) TO 'outputs/alerts_by_date.csv' (HEADER)
        """
    )


def main():
    print("Generating tables for critical dependabot alerts...")
    # create output folder if it doesn't exist
    if not os.path.exists("outputs"):
        os.makedirs("outputs")

    load_dotenv()

    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    GITHUB_ORGANISATION = os.getenv("GITHUB_ORGANISATION")

    generate_raw_alerts_table(
        github_organisation=GITHUB_ORGANISATION, github_token=GITHUB_TOKEN
    )

    generate_processed_alert_tables()
    print("Alert tables generated")


if __name__ == "__main__":
    main()
