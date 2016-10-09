#include "hash_func.h"

unsigned int RSHash(char* str, unsigned int len)
{  
   unsigned int b = 378551; 
   unsigned int a = 63689;  
   unsigned int hash = 0;  
   unsigned int i = 0;  
  
   for(i=0; i<len; str++, i++) {  
      hash = hash*a + (*str);  
      a = a*b;  
   }  
   return hash;  
}  
  
unsigned int JSHash(char* str, unsigned int len)  
{  
   unsigned int hash = 1315423911;  
   unsigned int i    = 0;  
  
   for(i=0; i<len; str++, i++) {  
      hash ^= ((hash<<5) + (*str) + (hash>>2));  
   }  
   return hash;  
}  
  
unsigned int PJWHash(char* str, unsigned int len)  
{  
   const unsigned int BitsInUnsignedInt = (unsigned int)(sizeof(unsigned int) * 8);  
   const unsigned int ThreeQuarters = (unsigned int)((BitsInUnsignedInt  * 3) / 4);  
   const unsigned int OneEighth = (unsigned int)(BitsInUnsignedInt / 8);  
   const unsigned int HighBits = (unsigned int)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);  
   unsigned int hash = 0;  
   unsigned int test = 0;  
   unsigned int i = 0;  
  
   for(i=0;i<len; str++, i++) {  
      hash = (hash<<OneEighth) + (*str);  
      if((test = hash & HighBits)  != 0) {  
         hash = ((hash ^(test >> ThreeQuarters)) & (~HighBits));  
      }  
   }  
  
   return hash;  
}  
  
unsigned int ELFHash(char* str, unsigned int len)  
{  
   unsigned int hash = 0;  
   unsigned int x    = 0;  
   unsigned int i    = 0;  
  
   for(i = 0; i < len; str++, i++) {  
      hash = (hash << 4) + (*str);  
      if((x = hash & 0xF0000000L) != 0) {  
         hash ^= (x >> 24);  
      }  
      hash &= ~x;  
   }  
   return hash;  
}  
  
unsigned int BKDRHash(char* str, unsigned int len)  
{  
   unsigned int seed = 131; /* 31 131 1313 13131 131313 etc.. */  
   unsigned int hash = 0;  
   unsigned int i    = 0;  
  
   for(i = 0; i < len; str++, i++)  
   {  
      hash = (hash * seed) + (*str);  
   }  
  
   return hash;  
}  
  
unsigned int SDBMHash(char* str, unsigned int len)  
{  
   unsigned int hash = 0;  
   unsigned int i    = 0;  
  
   for(i = 0; i < len; str++, i++) {  
      hash = (*str) + (hash << 6) + (hash << 16) - hash;  
   }  
  
   return hash;  
}  
  
unsigned int DJBHash(char* str, unsigned int len)  
{  
   unsigned int hash = 5381;  
   unsigned int i    = 0;  
  
   for(i = 0; i < len; str++, i++) {  
      hash = ((hash << 5) + hash) + (*str);  
   }  
  
   return hash;  
}  
  
unsigned int DEKHash(char* str, unsigned int len)  
{  
   unsigned int hash = len;  
   unsigned int i    = 0;  
  
   for(i = 0; i < len; str++, i++) {  
      hash = ((hash << 5) ^ (hash >> 27)) ^ (*str);  
   }  
   return hash;  
}  
  
unsigned int BPHash(char* str, unsigned int len)  
{  
   unsigned int hash = 0;  
   unsigned int i    = 0;  
   for(i = 0; i < len; str++, i++) {  
      hash = hash << 7 ^ (*str);  
   }  
  
   return hash;  
}  
  
unsigned int FNVHash(char* str, unsigned int len)  
{  
   const unsigned int fnv_prime = 0x811C9DC5;  
   unsigned int hash      = 0;  
   unsigned int i         = 0;  
  
   for(i = 0; i < len; str++, i++) {  
      hash *= fnv_prime;  
      hash ^= (*str);  
   }  
  
   return hash;  
}  
  
unsigned int APHash(char* str, unsigned int len)  
{  
   unsigned int hash = 0xAAAAAAAA;  
   unsigned int i    = 0;  
  
   for(i = 0; i < len; str++, i++) {  
      hash ^= ((i & 1) == 0) ? (  (hash <<  7) ^ (*str) * (hash >> 3)) :  
                               ((~((hash << 11) + (*str)) ^ (hash >> 5)));  
   }  
  
   return hash;  
}  

unsigned int HFLPHash(char *str,unsigned int len)  
{  
   unsigned int n=0;  
   unsigned int i;  
   char* b=(char *)&n;  
   for(i=0;i<len;++i) {  
     b[i%4]^=str[i];  
    }  
    return n%len;  
}  

unsigned int HFHash(char* str,unsigned int len)  
{  
   int result=0;  
   char* ptr=str;  
   int c;  
   int i=0;  
   for (i=1;(c=*ptr++);i++)  
   result += c*3*i;  
   if (result<0)  
      result = -result;  
   return result%len;  
}  
  
unsigned int StrHash( char *str, unsigned int len)  
{  
   (void)len;
   register unsigned int   h;  
   register unsigned char *p;  

   for(h=0,p=(unsigned char *)str;*p;p++) {  
       h=31*h+*p;  
   }  

     return h;  
}  
  
unsigned int TianlHash(char *str, unsigned int len)  
{  
   unsigned long urlHashValue=0;  
   int ilength=len;  
   int i;  
   unsigned char ucChar;  
   if(!ilength)  {  
       return 0;  
   }  

   if(ilength<=256)  {  
      urlHashValue=16777216*(ilength-1);  
  } else {   
      urlHashValue = 42781900080;  
  }  

  if(ilength<=96) {  
      for(i=1;i<=ilength;i++) {  
          ucChar=str[i-1];  
          if(ucChar<='Z'&&ucChar>='A')  {  
              ucChar=ucChar+32;  
          }  
          urlHashValue+=(3*i*ucChar*ucChar+5*i*ucChar+7*i+11*ucChar)%1677216;  
      }  
  } else  {  
      for(i=1;i<=96;i++)  
      {  
          ucChar=str[i+ilength-96-1];  
          if(ucChar<='Z'&&ucChar>='A')  
          {  
              ucChar=ucChar+32;  
          }  
          urlHashValue+=(3*i*ucChar*ucChar+5*i*ucChar+7*i+11*ucChar)%1677216;  
      }  
  }  
  return urlHashValue;  
  
 }  
