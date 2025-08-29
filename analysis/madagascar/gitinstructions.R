# Here's a **cheat-sheet card** you can keep handy for daily Git use in RStudio ????
# (copy this into a Markdown file like `GIT-CHEATSHEET.md` in your repo, so you always see it in GitHub/RStudio)
# 
# ---
#   
#   # ???? Git in RStudio - Quick Workflow
#   
#   ### 0. Starting a session
#   
#   * **Always Pull first (???)** ??? get the latest changes from GitHub.
# 
# ---
#   
#   ### 1. Make changes
#   
#   * Edit your R scripts, docs, etc.
# * Save your files.
# 
# ---
#   
#   ### 2. Commit (local checkpoint)
#   
#   1. Open the **Git tab** (top-right pane).
# 2. Check the boxes next to changed files (stage them).
# 3. Click **Commit**.
# 4. Write a short message, e.g.
# 
# ```
# fix: correct typo in Sri Lanka scraper
# feat: add ingest script for Madagascar
# docs: update README with new dataset
# ```
# 5. Click **Commit**.
# ???? This saves the snapshot **only on your computer**.
# 
# ---
#   
#   ### 3. Push (share to GitHub)
#   
#   * Click the **Push (???)** button in the Git tab.
# * This uploads your commits to GitHub.
# * Refresh GitHub ??? changes appear.
# 
# ---
#   
#   ### 4. Pull (get others' changes)
#   
#   * Click the **Pull (???)** button in the Git tab.
# * This downloads any changes from GitHub to your local copy.
# * Do this **at the start of every session**.
# 
# ---
#   
#   ### 5. Conflict? (rare at first)
#   
#   * If both you and GitHub changed the same lines, RStudio will highlight conflicts.
# * Open the file, pick the correct version, save, then commit + push again.
# 
# ---
#   
#   ### Common commit prefixes
#   
#   * `feat:` ??? new feature
# * `fix:` ??? bug fix
# * `docs:` ??? documentation only
# * `chore:` ??? maintenance / no code change
# 
# ---
#   
#   ### Handy Terminal commands (optional)
#   
#   ```bash
# git status            # what's changed / staged
# git log --oneline     # recent history
# git diff              # see what actually changed
# ```
# 
# ---
#   
#   ??? **The golden loop**
#   **Pull ??? Edit ??? Stage ??? Commit ??? Push**
#   
#   ---
#   
#   Do you want me to also make you a **visual one-page PDF card** (with icons for pull, commit, push) you can literally pin by your desk?
#   