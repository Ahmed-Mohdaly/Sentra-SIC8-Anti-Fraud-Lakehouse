# SIC8 — البداية بالعربي

ده **Repository مبدئية قابلة للتشغيل محليًا** لمشروع Fraud Detection، مش ادعاء إن كل خدمات AWS وDatabricks وPower BI اتربطت واتجربت. احنا حافظنا على Notebook القديمة، ومش هنغيّر Silver المنشورة على Databricks أو ملفات S3 الأصلية من غير موافقة.

**الفريق:** سيد R1 مسؤول Generator/Kafka/CDC/Online ML؛ أحمد Mohdaly R2 مسؤول Spark/Silver/Gold/Fraud؛ أحمد عبد الرازق R3 مسؤول Power BI/Frontend؛ أحمد إسلام R4 مسؤول Infrastructure/Airflow/Monitoring/API.

**تشغيل محلي آمن:** شغّل `python -m pip install -e '.[dev]'` وبعدها `make test` ثم `make demo-local`. هتلاقي النتائج في `build/demo/report.json`. دي بيانات تجربة `DEMO_FIXTURE` فقط، مش Kafka/MSK live ولا model LightGBM حقيقي. لو عايز تشغل الـAPI اقرأ README بالإنجليزي؛ محتاج Token محلي، ومش تنشرها Public.

**مهم:** `historical_ground_truth` هي إجابات المصدر الأصلي، مش نتيجة Fraud Detector. بنحافظ على Signed Amount وعلى العملة. Historical dataset مفيهاش GPS دقيق أو Terminal Failure مؤكد. الـSimulator محتاج يوفّرهم لاختبار المتطلبات. بيانات CDC ممكن توصل متأخرة؛ مينفعش نرمي المعاملة بسبب User/Card لسه موصلوش.

**اللي ناقص:** كود سيد الأصلي، موديل LightGBM الحقيقي، ملفات Databricks 01–07 المصدّرة، Dashboard Power BI الفعلية، صلاحيات Cloud/Logs واختبارات Replay. راجع `NEXT_HANDOFF.md`. ممنوع ترفع Keys/CVV/Raw Dataset على Git.
