const WORKTREE = '/Users/macmini/south-cinema-analytics/.claude/worktrees/dazzling-elbakyan'

module.exports = {
  apps: [
    {
      name: 'sca-frontend',
      cwd: `${WORKTREE}/frontend`,
      script: '/usr/local/opt/node@20/bin/npm',
      args: 'run dev',
      env: {
        PATH: `/usr/local/opt/node@20/bin:${process.env.PATH}`,
      },
    },
    {
      name: 'sca-backend',
      cwd: `${WORKTREE}/backend`,
      script: '/usr/local/bin/python3',
      args: '-m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload',
      env: {
        DATABASE_URL: 'postgresql://sca:sca@localhost:5432/sca',
      },
    },
  ],
}
