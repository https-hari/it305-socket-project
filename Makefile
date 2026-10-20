# Convenience wrapper. The graded Makefiles are the ones inside each Part*/ folder.
PARTS := Part1 Part2_Case1 Part2_Case2 Part2_Case2_Enhanced
DATA  ?= /data

.PHONY: all clean sync test
all: sync
	@for d in $(PARTS); do $(MAKE) -C $$d || exit 1; done

sync:                      # copy common/ into every Part*/ so each folder stays self-contained
	@./sync_common.sh > /dev/null

clean:
	@for d in $(PARTS); do $(MAKE) -C $$d clean; done

test: all                  # make test DATA=/path/to/raw-img
	python3 tools/verify.py $(DATA)
