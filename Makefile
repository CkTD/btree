# Copyright (C) 2019 zn
# 
# This file is part of btree.
# 
# btree is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# btree is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with btree.  If not, see <http://www.gnu.org/licenses/>.

CFLAGS = -g -I.

.PHONY: all clean

all: btree_test table_test

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<
  
btree_test: btree.o btree_test.o
	$(CC) $(CFLAGS) -o $@ $^

table_test: btree.o table.o table_test.o
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f *.o btree_test table_test