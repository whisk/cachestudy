# Cache Study

A simulator tool to study web application caching:

* Various effects like cache stampede, stale sets, etc.
* Improvement techniques like dynamic TTL extension, etc.

## TODO

- [x] Gather simulation parameters into a single data structure
- [x] Make simulation parameters fully controllable via command-line arguments
- [x] Add random seed
- [ ] Beautify logger format
- [x] Write simulation parameters into the journal (in the comments section), with the current time
- [x] Display all important charting parameters on the plot
- [ ] Introduce some kind of versioning and write the version into the journal
- [ ] Simulate TTL reads properly
- [ ] Extract request and response generators
- [ ] Streamline simulation parameters injections
- [ ] Separate key-value logic and simulation logic
- [ ] Support multiple events per one request
