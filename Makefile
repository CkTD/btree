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

CFLAGS = -g -I. -Wall
LDFLAGS = -lpthread

.PHONY: all clean

all: btree_test1 btree_test2 table_test1 table_test2 table_test3

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<
  
btree_test1: btree.o btree_test1.o
	$(CC) $(LDFLAGS) -o $@ $^

btree_test2: btree.o btree_test2.o
	$(CC) $(LDFLAGS) -o $@ $^

table_test1: btree.o table.o table_test1.o
	$(CC) $(LDFLAGS) -o $@ $^

table_test2: btree.o table.o table_test2.o
	$(CC) $(LDFLAGS) -o $@ $^

table_test3: btree.o table.o table_test3.o
	$(CC) $(LDFLAGS) -o $@ $^

clean:
	rm -f *.o btree_test{1,2} table_test{1,2,3}
