#include <gtest/gtest.h>
#include <unistd.h>
#include <stdlib.h>
#include <string>
#include <map>
#include <iostream>
#include <errno.h>

extern "C" {
#include "./db.h"
}

using namespace std;

const char *gDbPath = "./data";
const int gCount = 100000;
//const int gCount = 900;

static size_t l_ksize = 32, l_vsize = 256;
static char kpattern[512], vpattern[40960], l_prefix[512];
static void test_init()
{
    int i;

    for (i = 0; i < l_ksize; i++) {
        kpattern[i] = 'k';
    }
    kpattern[i] = '\0';

    for (i = 0; i < l_vsize; i++) {
        vpattern[i] = 'v';
    }
    vpattern[i] = '\0';
}

static string RandomString() {
}

class TestDB : public testing::Test {
public:
  TestDB() : db_(NULL) {
  }

protected:
  virtual void SetUp() {
    string cmd = string("rm -fr ") + gDbPath;
    system(cmd.c_str());
    db_ = HIDB2(db_open)(gDbPath, NULL);
    HIDB2(db_run)(db_);
    kv_.clear();
  }

  virtual void TearDown() {
    HIDB2(db_checkpoint)(db_);
    HIDB2(db_close)(db_);
    // 最后要删除测试数据目录
    string cmd = string("rm -fr ") + gDbPath;
    //system(cmd.c_str());
    kv_.clear();
  }

  map<string, string> kv_;
  db_t *db_;
};

// 基本的put、get测试
TEST_F(TestDB, PutGetTest) {
  int r = 0, i, ks, vs;
  char *kdata, *vdata;
  map<string, string>::iterator iter;

  // 先写入数据
  for (i = 0; i < gCount; i++) {
    kdata = (char*)malloc(l_ksize + 32);
    vdata = (char*)malloc(l_vsize + 32);

    sprintf(kdata, "%s-%d", kpattern, i);
    sprintf(vdata, "%s-%d", vpattern, i);

    ks = strlen(kdata);
    vs = strlen(vdata);

    r = HIDB2(db_put)(db_, kdata, ks, vdata, vs);

    ASSERT_EQ(r, 0);

    // 记录下写入的KV
    kv_[kdata] = vdata;
  }

  // 再依次获取，看是否成功
  iter = kv_.begin();
  for (; iter != kv_.end(); ++iter) {
    string k = iter->first;
    string v = iter->second;

    r = HIDB2(db_get)(db_, (char*)k.c_str(), k.size(), &vdata, (uint32_t*)&vs, NULL);

    ASSERT_EQ(r, 0);

    ASSERT_TRUE(v == string(vdata, vs));
  }

  // 删除KV数据
  iter = kv_.begin();
  for (; iter != kv_.end(); ++iter) {
    string k = iter->first;

    r = HIDB2(db_del)(db_, (char*)k.c_str(), k.size());

    ASSERT_EQ(r, 0);
  }

  // 删除之后再检查数据
  iter = kv_.begin();
  for (; iter != kv_.end(); ++iter) {
    string k = iter->first;

    r = HIDB2(db_get)(db_, (char*)k.c_str(), k.size(), &vdata, (uint32_t*)&vs, NULL);

    ASSERT_NE(r, 0) << "exist key: " << k << "\n";
  }
}

// 测试mput
TEST_F(TestDB, MPutGetTest) {
  int r = 0, i, ks, vs, ks2, vs2;
  char *kdata, *vdata;
  char *kdata2, *vdata2;
  kvec_t *kvs;
  map<string, string>::iterator iter;

  // 先写入数据
  for (i = 0; i < gCount; i++) {
    kvs = (kvec_t*)malloc(sizeof(kvec_t) * 2);

    kdata = (char*)malloc(l_ksize + 32);
    vdata = (char*)malloc(l_vsize + 32);

    kdata2 = (char*)malloc(l_ksize + 32);
    vdata2 = (char*)malloc(l_vsize + 32);

    sprintf(kdata, "%s-%d", kpattern, i);
    sprintf(vdata, "%s-%d", vpattern, i);

    sprintf(kdata2, "%s-%d", kpattern, i + gCount);
    sprintf(vdata2, "%s-%d", vpattern, i + gCount);

    ks = strlen(kdata);
    vs = strlen(vdata);
    ks2 = strlen(kdata2);
    vs2 = strlen(vdata2);

    kvs[0].k  = kdata;
    kvs[0].v  = vdata;
    kvs[0].ks = ks;
    kvs[0].vs = vs;

    kvs[1].k  = kdata2;
    kvs[1].v  = vdata2;
    kvs[1].ks = ks2;
    kvs[1].vs = vs2;

    r = HIDB2(db_mput)(db_, kvs, 2);

    ASSERT_EQ(r, 0);

    // 记录下写入的KV
    kv_[kdata] = vdata;
    kv_[kdata2] = vdata2;
  }

  // 再依次获取，看是否成功
  iter = kv_.begin();
  for (; iter != kv_.end(); ++iter) {
    string k = iter->first;
    string v = iter->second;

    r = HIDB2(db_get)(db_, (char*)k.c_str(), k.size(), &vdata, (uint32_t*)&vs, NULL);

    ASSERT_EQ(r, 0);

    ASSERT_TRUE(v == string(vdata, vs));
  }

  // mdel删除
  iter = kv_.begin();
  for (; iter != kv_.end(); ) {
    kvs = (kvec_t*)malloc(sizeof(kvec_t) * 2);

    string k = iter->first;
    ++iter;
    string k2 = iter->first;
    ++iter;

    ks = k.length();
    ks2 = k2.length();

    kdata  = (char*)malloc(ks);
    kdata2  = (char*)malloc(ks2);
    sprintf(kdata, "%s", k.c_str());
    sprintf(kdata2, "%s", k2.c_str());

    kvs[0].k  = kdata;
    kvs[0].ks = ks;

    kvs[1].k  = kdata2;
    kvs[1].ks = ks2;

    r = HIDB2(db_mdel)(db_, kvs, 2);
    ASSERT_EQ(r, 0);
    //cout << "del " << kdata << " & " << kdata2 << "\n";
  }

  // 再依次获取，看是否成功
  iter = kv_.begin();
  for (; iter != kv_.end(); ++iter) {
    string k = iter->first;

    r = HIDB2(db_get)(db_, (char*)k.c_str(), k.size(), &vdata, (uint32_t*)&vs, NULL);

    ASSERT_NE(r, 0) << "exist key: " << k;
  }
}

// 测试prefix相关的操作
TEST_F(TestDB, POpTest) {
  int r = 0, i, ks, vs, ks2, vs2;
  char *kdata, *vdata;
  char *kdata2, *vdata2;
  kvec_t *kvs;
  char *k, *v, *kt, *vt;
  iter_t *it;
  map<string, string>::iterator iter;
  map<string, string> kv, kv2;

  // 先写入数据
  for (i = 0; i < gCount; i++) {
    kvs = (kvec_t*)malloc(sizeof(kvec_t) * 2);

    kdata = (char*)malloc(l_ksize + 32);
    vdata = (char*)malloc(l_vsize + 32);

    kdata2 = (char*)malloc(l_ksize + 32);
    vdata2 = (char*)malloc(l_vsize + 32);

    sprintf(kdata, "%s-%d", kpattern, i);
    sprintf(vdata, "%s-%d", vpattern, i);

    sprintf(kdata2, "%s-%d", kpattern, i + gCount);
    sprintf(vdata2, "%s-%d", vpattern, i + gCount);

    ks = strlen(kdata);
    vs = strlen(vdata);
    ks2 = strlen(kdata2);
    vs2 = strlen(vdata2);

    kvs[0].k  = kdata;
    kvs[0].v  = vdata;
    kvs[0].ks = ks;
    kvs[0].vs = vs;

    kvs[1].k  = kdata2;
    kvs[1].v  = vdata2;
    kvs[1].ks = ks2;
    kvs[1].vs = vs2;

    r = HIDB2(db_mput)(db_, kvs, 2);

    ASSERT_EQ(r, 0);

    // 记录下写入的KV
    kv_[kdata] = vdata;
    kv_[kdata2] = vdata2;

    kv[kdata] = vdata;
    kv[kdata2] = vdata2;

    kv2[kdata] = vdata;
    kv2[kdata2] = vdata2;
  }

  // pget按照共同的前缀来查询数据，应该能拿到所有数据
  it = HIDB2(db_pget)(db_, kpattern, strlen(kpattern));
  while (HIDB2(db_iter)(it, &k, &ks, &v, &vs, NULL) == 0) {
    string key(k, ks);

    // 拿到一个数据从记录中删一条
    kv.erase(key);
    free(k);
    free(v);
  }
  HIDB2(db_destroy_it)(it);
  // 出了循环之后记录应该被删空了
  ASSERT_TRUE(kv.empty()) << "map size: " << kv.size();

  // 使用空字符串来获取数据,应该能拿到所有数据
  //it = HIDB2(db_pget)(db_, "", 0);
  it = HIDB2(db_pget)(db_, NULL, 0);
  while (HIDB2(db_iter)(it, &k, &ks, &v, &vs, NULL) == 0) {
    string key(k, ks);

    // 拿到一个数据从记录中删一条
    kv2.erase(key);
    free(k);
    free(v);
  }
  HIDB2(db_destroy_it)(it);
  // 出了循环之后记录应该被删空了
  ASSERT_TRUE(kv2.empty()) << "map size: " << kv2.size();

//  return;
  // pdel删除数据
  r = HIDB2(db_pdel)(db_, kpattern, strlen(kpattern));
  ASSERT_EQ(r, 0);


  // 再次遍历原先保存的map，此时应该查不到数据了
  iter = kv_.begin();
  for (; iter != kv_.end(); ++iter) {
    string k = iter->first;

    r = HIDB2(db_get)(db_, (char*)k.c_str(), k.size(), &vdata, (uint32_t*)&vs, NULL);

    ASSERT_NE(r, 0) << "exist key: " << k << "\n";
  }
}

// 测试针对空数据库进行查询的操作
TEST_F(TestDB, GetEmptyDB) {
  int r = 0, i, ks, vs;
  char *kdata, *vdata;
  iter_t *it;

  // 简单的get操作
  for (i = 0; i < gCount; i++) {
    kdata = (char*)malloc(l_ksize + 32);
    ks = strlen(kdata);

    sprintf(kdata, "%s-%d", kpattern, i);
    r = HIDB2(db_get)(db_, kdata, ks, &vdata, (uint32_t*)&vs, NULL);
    ASSERT_NE(r,0);
  }

  // pget遍历
  it = HIDB2(db_pget)(db_, kpattern, strlen(kpattern));
  while (HIDB2(db_iter)(it, &kdata, &ks, &vdata, &vs, NULL) == 0) {
    // 空数据库不应该走进来
    ASSERT_TRUE(false);
  }
  HIDB2(db_destroy_it)(it);
}

// 测试在del之后，写大量binlog造成memtable的切换，再去get是否能拿到数据
TEST_F(TestDB, GetAfterDel) {
  int r = 0, i, ks, vs;
  char *kdata, *vdata;

  string k = "k";
  string v = "v";

  // 首先set这对kv数据
  kdata = (char*)malloc(l_ksize + 32);
  vdata = (char*)malloc(l_vsize + 32);

  sprintf(kdata, "%s", k.c_str());
  sprintf(vdata, "%s", v.c_str());

  ks = strlen(kdata);
  vs = strlen(vdata);

  r = HIDB2(db_put)(db_, kdata, ks, vdata, vs);

  ASSERT_EQ(r, 0);

  // 其次构造出一堆无用的del操作，目的为了binlog足够大，
  // 这样memtable就会到imq中
  for (i = 0; i < gCount; i++) {
    kdata = (char*)malloc(l_ksize + 32);

    sprintf(kdata, "%s-%d", kpattern, i);

    ks = strlen(kdata);
    r = HIDB2(db_del)(db_, kdata, ks);

    ASSERT_EQ(r, 0);
  }

  // 上面的操作完毕之后，应该切换了memtable

  // 此时再次去get操作，应该能拿到数据
  r = HIDB2(db_get)(db_, (char*)k.c_str(), k.size(), &vdata, (uint32_t*)&vs, NULL);
  ASSERT_EQ(r, 0);

  // 然后执行del操作
  r = -1;
  r = HIDB2(db_del)(db_, (char*)k.c_str(), k.size());
  ASSERT_EQ(r, 0);

  // 此时再次去get操作，拿不到数据
  r = HIDB2(db_get)(db_, (char*)k.c_str(), k.size(), &vdata, (uint32_t*)&vs, NULL);
  ASSERT_NE(r, 0);

  // 关闭db
  HIDB2(db_checkpoint)(db_);
  HIDB2(db_close)(db_);

  // 重新打开db
  db_ = HIDB2(db_open)(gDbPath, NULL);
  HIDB2(db_run)(db_);

  // 此时再次去get操作，拿不到数据
  r = HIDB2(db_get)(db_, (char*)k.c_str(), k.size(), &vdata, (uint32_t*)&vs, NULL);
  ASSERT_NE(r, 0);
}

// 测试数据库的异常恢复
TEST(TestRecover, TestRecover) {
  map<string, string> kv;
  string cmd = string("rm -fr ") + gDbPath;
  system(cmd.c_str());
  int r = 0, i, ks, vs;
  char *kdata, *vdata;

  // 先生成测试KV
  for (i = 0; i < gCount; i++) {
    kdata = (char*)malloc(l_ksize + 32);
    vdata = (char*)malloc(l_vsize + 32);

    sprintf(kdata, "%s-%d", kpattern, i);
    sprintf(vdata, "%s-%d", vpattern, i);
    // 记录下写入的KV
    kv[kdata] = vdata;

    free(kdata);
    free(vdata);
  }

  // fork进程来操作数据库
  pid_t pid = fork();
  if (pid == 0) {
    // child,插入数据
    cout << "in child: " << kv.size() << endl;    
    db_t *db;
    db = HIDB2(db_open)(gDbPath, NULL);
    HIDB2(db_run)(db);
    for (i = 0; i < gCount; i++) {
      kdata = (char*)malloc(l_ksize + 32);
      vdata = (char*)malloc(l_vsize + 32);

      sprintf(kdata, "%s-%d", kpattern, i);
      sprintf(vdata, "%s-%d", vpattern, i);

      HIDB2(db_put)(db, kdata, strlen(kdata), vdata, strlen(vdata));
    }

    // 需要等待几秒，数据才能落地保存到磁盘
    sleep(5);
    // 最后不close直接退出，模拟进程异常退出的场景
  } else if (pid > 0) {
    // 父进程
    waitpid(pid,NULL,0);

    // 检查数据是否都保存下来了
    db_t *db;
    db = HIDB2(db_open)(gDbPath, NULL);
    HIDB2(db_run)(db);
    map<string, string>::iterator iter;
    iter = kv.begin();
    for (; iter != kv.end(); ++iter) {
      string k = iter->first;
      string v = iter->second;

      r = HIDB2(db_get)(db, (char*)k.c_str(), k.size(), &vdata, (uint32_t*)&vs, NULL);

      ASSERT_EQ(r, 0) << "key not exist: " << k << "\n";

      ASSERT_TRUE(v == string(vdata, vs)) << "key " << k << " data error: " << vdata << ", expected: " << v << endl;
    }
  } else if (pid < 0) {
    ASSERT_TRUE(false) << "fork error";
  }
}
#if 0
// 简单的用例，只是为了调试用的
TEST(Test, Test) {
  int r = 0, i, j, ks, vs;
  char *kdata, *vdata;
  db_t *db;
  db = HIDB2(db_open)(gDbPath, NULL);
  HIDB2(db_run)(db);
  for (j = 0; j < 1; j++) {
    for (i = 0; i < 1000000; i++) {
      kdata = (char*)malloc(l_ksize + 32);
      vdata = (char*)malloc(l_vsize + 32);

      sprintf(kdata, "%s-%d", kpattern, i);
      sprintf(vdata, "%s-%d", vpattern, i);

      HIDB2(db_put)(db, kdata, strlen(kdata), vdata, strlen(vdata));
    }
  }
}
#endif
int main(int argc, char *argv[]) {
    test_init();
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
