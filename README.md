# نظام كشف الاحتيال المالي — Big Data Streaming

مشروع مادة عملي وبسيط يرسل معاملات بنكية إلى **Apache Kafka**، ويحللها **Spark Structured Streaming** بقواعد واضحة ونموذج XGBoost. بقي النموذج الأصلي في `pipeline.joblib`، وتستخدم الحاوية نسخة الأشجار المحمولة `pipeline.xgb.json` للاستدلال دون مكتبات ML ضخمة. تُحفظ الأحداث الخام بصيغة Parquet في **Hadoop HDFS**، وتُحفظ فهارس العرض والتنبيهات في SQLite مشتركة وتظهر فوراً في لوحة عربية.

## ما الذي يكشفه النظام؟

1. مبلغ كبير (`amount >= 10,000` افتراضياً).
2. تكرار معاملات للعميل نفسه خلال 10 دقائق (`>= 4`).
3. نتيجة نموذج XGBoost الموجود في `pipeline.joblib` والمدرّب على بيانات بطاقات الائتمان (الحقول `V1` إلى `V28`, `Amount`).

> النموذج ليس Pipeline معالجة؛ لذلك يولّد الملف التجريبي هذه الميزات العددية بالترتيب الصحيح. في البيانات الحقيقية يجب أن تأتي ميزات `V1..V28` من نفس preprocessing الذي استُخدم عند التدريب.

## المعمارية

```text
مولّد/لوحة البيانات
        │ JSON events
        ▼
Kafka (bank-transactions, 3 partitions)
        │
        ▼
Spark Structured Streaming + XGBoost
        ├──► HDFS: raw Parquet + streaming checkpoint
        └──► SQLite: transactions + fraud_alerts (لوحة العرض)
                         │
                         ▼
                  FastAPI Dashboard
```

الحاويات: `zookeeper`, `kafka`, `kafka-init`, `kafka-ui`, `itvdelab` (Hadoop HDFS/YARN)، `spark-stream`، و`dashboard`. توجد خدمة PostgreSQL مساندة لتهيئة Hive داخل صورة المقرر، وخدمة قصيرة لتهيئة صلاحيات مساحة التخزين المشتركة.

## التشغيل السريع عبر Docker

```powershell
docker compose up --build -d
docker compose ps
```

ثم افتح:

- لوحة المشروع: http://localhost:8000
- Kafka UI: http://localhost:8080
- Hadoop NameNode: http://localhost:9870

من لوحة المشروع اختر سيناريو من القائمة المنسدلة واضغط **بدء تشغيل النظام**. تبدأ المحاكاة من شاشة نظيفة وترسل 5 معاملات تقريباً كل ثانية؛ يستهلك Spark الرسائل في micro-batches، ويكتب نسخة خام في HDFS ثم يسجل التنبيهات تدريجياً في قاعدة العرض. استخدم زر **إيقاف** لإنهاء توليد البيانات مع بقاء النتائج ظاهرة. يحتوي شريط التنقل على بطاقة النموذج، شرح المعمارية، وViewer يعرض توثيق [PROJECT.md](docs/PROJECT.md) مباشرة. ملفات موصل Kafka مضمنة في صورة المشروع، لذلك لا يحتاج التشغيل التالي إلى Maven أو Docker Hub.

## دفعات البيانات التجريبية

تظهر في اللوحة مجموعات: `mixed` (121)، `normal_only` (90)، `high_value` (81)، `velocity` (103)، و`model` (84). ويمكن النشر من الطرفية أيضاً:

```powershell
docker compose run --rm dashboard python -m src.stream_producer --preset mixed --interval 0.1
```

لرؤية السجلات والتحقق من HDFS:

```powershell
docker compose logs -f spark-stream
docker compose exec namenode hdfs dfs -ls -R /fraud
```

ولإيقاف المشروع مع إبقاء البيانات:

```powershell
docker compose down
```

استخدم `docker compose down -v` فقط إذا أردت حذف بيانات MongoDB وHDFS وإعادة المشروع من الصفر.

## الملفات المهمة

- `src/stream_producer.py`: منتج Kafka.
- `src/fraud_streaming_job.py`: مهمة Spark المستمرة وطبقة القواعد وML.
- `src/xgb_predictor.py`: استدلال خفيف من أشجار XGBoost المصدرة.
- `src/storage.py`: تخزين مشترك صغير للوحة والتنبيهات.
- `src/api.py`: API ولوحة التحكم ونشر الدفعات.
- `src/fraud_spark_job.py`: المسار الدفعي القديم المعتمد على MongoDB، أبقي كمرجع للمقارنة.
- `docker-compose.yml`: البنية الكاملة للخدمات.

> ملاحظة أكاديمية: النموذج مبني على ميزات `V1..V28` و`Amount`. في نظام بنكي حقيقي يجب أن تأتي هذه الميزات من نفس preprocessing المستخدم أثناء تدريب النموذج، مع تشفير واتصالات مصادق عليها وسياسات احتفاظ بالبيانات.
