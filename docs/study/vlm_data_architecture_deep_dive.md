# 📘 [심층 학습지] VLM 데이터 파이프라인 아키텍처 마스터 (Hadoop, Spark, DuckDB)

이 학습지는 단순한 요약을 넘어, **현대적인 데이터 엔지니어링의 핵심 이론을 단계별로 깨우칠 수 있도록 설계**되었습니다. 각 기술의 공식 문서에 기반한 내부 작동 원리를 심층적으로 탐구합니다.

---

## 🎯 학습 목표
1.  **HDFS Read/Write 경로**를 통해 분산 저장소의 메타데이터 관리 원리를 이해한다.
2.  **YARN의 리소스 할당 라이프사이클**을 파악하여 분산 클러스터의 자원 관리를 최적화한다.
3.  **Spark의 지연 평가 및 DAG 최적화**를 통해 효율적인 실행 계획 수립 원리를 습득한다.
4.  **DuckDB의 벡터화 연산(Vectorized Execution)**이 전통적인 DB와 어떻게 다른지 기술적으로 증명한다.

---

## 🟢 모듈 1: 분산 저장 기법 (Hadoop & MinIO)

### **레슨 1.1: HDFS의 읽기/쓰기 해부 (The Anatomy of Read/Write)**
클라이언트가 파일을 읽을 때 일어나는 현상을 단계별로 학습합니다.

1.  **메타데이터 조회 (Step 1):** 클라이언트가 NameNode에게 파일의 블록 위치를 요청합니다. (이때 파일 시스템 네임스페이스는 NameNode의 메모리에 상주하여 즉각적인 응답을 보장합니다.)
2.  **블록 소스 확인 (Step 2):** NameNode는 데이터가 위치한 최적의 DataNode 목록을 반환합니다. (네트워크 거리(Rack Awareness)를 고려하여 가장 가까운 노드를 우선 배정합니다.)
3.  **스트리밍 읽기 (Step 3):** 클라이언트가 DataNode와 직접 통신하여 데이터를 패킷 단위로 읽어옵니다.

### **레슨 1.2: 클라우드 네이티브 스토리지 최적화 (S3A & Vectored IO)**
전통적인 HDFS를 넘어 MinIO와 같은 오브젝트 스토리지와 통신할 때의 최신 이론입니다.
*   **Vectored IO 기술:** Parquet 파일은 컬럼 단위로 저장되어 있어 읽어야 할 부분이 중간중간 떨어져 있습니다. Vectored IO는 이 불연속적인 범위들을 하나의 요청으로 묶어 병렬로 가져오는 'Scatter/Gather' 방식을 사용합니다.

> **💡 Checkpoint: 질문**
> *   "파일 시스템의 메타데이터가 NameNode의 메모리 크기를 초과할 만큼 많아지면 어떤 문제가 발생하며, 이를 해결하기 위한 Hadoop 3.x의 전략은 무엇입니까?"

---

## 🔵 모듈 2: 분산 처리 기법 (Apache Spark)

### **레슨 2.1: 논리적 계획에서 물리적 실행까지 (Catalyst & DAG)**
Spark는 어떻게 우리가 작성한 코드를 최적의 실행 경로로 바꿀까요?

1.  **Logical Plan 수립:** 사용자의 코드를 트리 구조의 논리적 연산으로 변환합니다.
2.  **Catalyst Optimizer 최적화:** 불필요한 필터링을 미리 수행하거나(Projection Pushdown), 조인 순서를 변경하여 데이터 이동을 최소화합니다.
3.  **Physical Plan 생성:** 실제로 분산 노드에서 실행할 물리적인 쿼리 계획(RDD 레벨)을 수립합니다.
4.  **DAG(비순환 그래프) 생성:** 데이터 이동(Shuffle)이 일어나는 지점을 기준으로 스테이지(Stage)를 나누어 병렬 작업(Task)을 할당합니다.

### **레슨 2.2: 셔플(Shuffle)과 파티셔닝(Partitioning)**
*   Spark 성능 병목의 90%는 **셔플**에서 발생합니다. 셔플은 네트워크를 통해 노드 간 데이터를 대규모로 교환하는 과정입니다. 데이터를 적절히 파티셔닝(해시, 레인지)하여 셔플 발생 빈도를 줄이는 것이 숙련된 데이터 엔지니어의 핵심 역량입니다.

---

## 🟠 모듈 3: 현대적 분석용 엔진 (DuckDB)

### **레슨 3.1: 벡터화 연산 vs 전통적 연산 (Vectorized vs Volcano Model)**
전통적인 DB(PostgreSQL 등)의 Volcano Model과 DuckDB의 차이를 분석합니다.
*   **Volcano Model (Tuple-at-a-time):** "한 줄 읽고 연산하고, 다음 줄 읽고 연산하고..." 이 방식은 CPU가 명령어를 미리 처리하는 파이프라이닝 기능을 방해하고 분기 예측 실패율을 높입니다.
*   **Vectorized Model (DuckDB):** 데이터를 약 2000개의 '청크(Chunk)' 단위로 한꺼번에 읽어 CPU의 L1/L2 캐시에 가둡니다. 캐시 적중률(Cache Hit Rate)을 극대화하여 동일한 CPU 클럭에서 수배 이상의 연산 속도를 냅니다.

### **레슨 3.2: Row Group & Columnar Storage**
*   DuckDB의 Parquet 읽기 이론: 파일을 122,880행 단위인 **Row Group**으로 쪼개고, 각 그룹 내부는 **컬럼 단위**로 저장합니다. 이 구조 덕분에 쿼리에 필요한 특정 컬럼만 빠르게 스캔할 수 있으며, 여러 스레드가 서로 다른 Row Group을 독립적으로 처리하는 병렬성이 가능해집니다.

---

## 🛠️ 실습 과제: VLM 파이프라인 아키텍처 고도화

현재 구축된 'Dagster + DuckDB + MinIO' 환경에 배운 이론을 적용해 봅시다.

1.  **데이터 가공 단계:** Dagster에서 대규모 CCTV 이미지 전처리 시, Spark의 **지연 평가(Lazy Evaluation)**를 활용해 중복 연산을 제거하는 로직을 어떻게 설계할 것인가?
2.  **데이터 저장 단계:** MinIO 저장 시 **Manifest Committer** 설정을 통해 파일 쓰기 정합성을 높이는 방법은 무엇인가?
3.  **데이터 분석 단계:** DuckDB에서 **S3A Vectored IO**를 활성화하여 Parquet 파일 분석 속도를 2배 이상 높이는 설정값은 무엇인가?

---

## 📚 참고 공식 문서 이론 리스트
*   Hadoop 3.3.5: [HDFS Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
*   Spark: [Cluster Mode Overview & Scheduler](https://spark.apache.org/docs/latest/cluster-overview.html)
*   DuckDB: [DuckDB Internals & Parallelism](https://duckdb.org/docs/guides/performance/parallelism)

가이드에 따라 이론을 반복 숙달하고, 실제 터미널 환경에서 `nlm query`를 통해 더 깊은 기술적 의구심을 해결해 보세요.
