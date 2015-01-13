#include <sys/types.h>
#include <stdio.h>
#include <db.h>
#include <stdlib.h>
#include <string.h>

const char *dbfile = "testme.db";
const char *prog;

int main(int argc, char **argv)
{
  DB *dbp;
  int ret;

  prog = argv[0];
  if (argc>1) {
    dbfile = argv[1];
  }

  if ((ret = db_create(&dbp, NULL, 0)) != 0) {
    fprintf(stderr, "db_create: %s\n", db_strerror(ret));
    exit (1);
  }
  //flags: e.g. DB_CREATE, DB_TRUNCATE, DB_RDONLY, ?DB_THREAD
  if ((ret = dbp->open(dbp, NULL, dbfile, NULL, DB_BTREE, DB_CREATE|DB_TRUNCATE, 0664)) != 0) {
    dbp->err(dbp, ret, "%s: open failed for dbfile '%s'", prog, dbfile);
    goto err;
  }
  //dbp->truncate();
  fprintf(stderr, "%s: created dbfile %s\n", prog, dbfile);

  //-- store key+value pairs
  int i;
  for (i=2; i < argc; i += 2) {
    DBT key,val;
    memset(&key, 0, sizeof(key));
    memset(&val, 0, sizeof(val));
    key.data = argv[i];
    key.size = strlen(key.data);
    val.data = ((i+1) < argc ? argv[i+1] : "");
    val.size = strlen(val.data);

    if ((ret = dbp->put(dbp, NULL, &key, &val, 0)) != 0) {
      dbp->err(dbp, ret, "%s: error in DB->put for key '%s'", prog, key.data);
      goto err;
    } else {
      printf("%s[%s]: put(%s->%s)\n", prog, dbfile, (char*)key.data, (char*)val.data);
    }

    if ((ret = dbp->get(dbp, NULL, &key, &val, 0)) == 0) {
      printf("%s[%s]: get(", prog,dbfile);
      fwrite(key.data, key.size, 1, stdout);
      fputs(") -> ", stdout);
      fwrite(val.data, val.size, 1, stdout);
      fputs("\n", stdout);
    }
    else {
      dbp->err(dbp, ret, "%s[%s]: DB->get(%s)", prog, dbfile, (char*)key.data);
      goto err;
    }

  }

  //-- close
  int t_ret;
 err:
  if ((t_ret = dbp->close(dbp, 0)) != 0 && ret == 0)
    ret = t_ret; 

  return ret;
}
