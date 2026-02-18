# NovaStor Task Completion Checklist

## Before Marking Task Complete

### 1. Code Quality
- [ ] Run `make check` (fmt + vet + lint) - all must pass
- [ ] Run `make test` - all tests must pass with race detection
- [ ] No linting rule violations
- [ ] No disabled linter rules in code

### 2. Testing
- [ ] Unit tests added/updated for new code
- [ ] Table-driven tests with `t.Run()` subtests
- [ ] Both success and error paths tested
- [ ] Integration tests updated if needed

### 3. Documentation
- [ ] API changes documented
- [ ] CRD changes documented with examples
- [ ] Architecture/docs updated if behavior changed
- [ ] Run `make docs-build` - must pass in strict mode

### 4. Git
- [ ] GitHub issue created and referenced
- [ ] Branch created in worktree (NOT from main worktree)
- [ ] Commit follows format: `[Type] Summary\n\nDetails\n\nResolves #NUM`
- [ ] No AI attribution in commits
- [ ] Changes committed with proper authorship

### 5. Security
- [ ] No secrets committed
- [ ] Input validation on external boundaries
- [ ] Errors properly wrapped with context
- [ ] Internal errors logged, generic errors returned

## After PR Merge
- [ ] Comment on GitHub issue with implementation summary
- [ ] Close issue when PR is merged
- [ ] Clean up worktree and delete branch
