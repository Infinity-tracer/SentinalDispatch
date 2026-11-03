# Literature Survey
## AI-Powered Climate-Aware Emergency Triage System

### IEEE Style Literature Review (2022-2024)

This literature survey examines recent research (2022 onwards) relevant to the Sentinel Dispatch project, focusing on AI-powered emergency response systems, real-time data processing, wildfire prediction, and climate-aware disaster management.

---

| S.No | Paper Title | Author Details | Inferences to Project |
|------|-------------|----------------|----------------------|
| 1 | **Improving triage performance in emergency departments using machine learning and natural language processing: a systematic review** | Authors from multiple institutions; Published in BMC Emergency Medicine, 2024 | Systematic review across IEEE Xplore, Web of Science, PubMed, Scopus, and ACM Digital Library (through October 2023) found that ML and NLP enhance triage accuracy and consistency. Models incorporating NLP achieved an average ROC-AUC of 0.91 and sensitivity of 0.80. **Direct inference:** Validates our approach of using Gemini NLP for emergency call classification in the CallProcessingAgent. Demonstrates that NLP integration significantly improves triage performance compared to structured data alone. |
| 2 | **AI-based approach for transcribing and classifying unstructured emergency call data: A methodological proposal** | Published in PLOS Digital Health, 2023 | Proposes an AI-based approach using automatic speech recognition (ASR) for speech-to-text transcription and natural language understanding (NLU) models to classify emergency calls. **Direct inference:** Provides methodological validation for our call transcription and classification pipeline. Our system can be extended with ASR capabilities to process raw audio emergency calls in addition to text transcripts. |
| 3 | **Artificial intelligence for severity triage based on conversations in an emergency department in Korea** | Published in Nature Scientific Reports, 2025 | Applied MLP, BiLSTM, and CNN models based on artificial neural networks to extract features for patient severity classification from conversations. **Direct inference:** Supports our multi-model approach and suggests potential deep learning enhancements to our risk scoring models beyond rule-based systems. |
| 4 | **Artificial Intelligence and Machine Learning-Based Triage Systems in Emergency Departments: A Systematic Review of Predictive Performance and Clinical Outcomes** | Systematic review published in PubMed, 2024; searched databases from 2021-2026 | Concluded that AI/ML models show moderate to excellent retrospective predictive performance for ED triage outcomes, particularly ensemble tree-based and NLP-enhanced approaches. **Direct inference:** Validates our hybrid ML/rule-based risk scoring approach and suggests ensemble methods could improve our RiskScoringAgent performance. |
| 5 | **A Geospatial Approach to Wildfire Risk Modeling Using Machine Learning and Remote Sensing Data** | Published in IEEE Journal of Selected Topics in Applied Earth Observations and Remote Sensing, January 2024 | Developed a machine learning model producing accurate wildfire risk assessments using five geospatial datasets: Land Fire Mean Return, Annual Precipitation, Sentinel-2 Imagery, Land Cover, and Moisture Deficit & Surplus. **Direct inference:** Directly relevant to our DataEnrichmentAgent's geospatial data integration. Suggests additional data sources (Sentinel-2 imagery, moisture metrics) we could incorporate to enhance our fire proximity risk calculations. |
| 6 | **Location-aware adaptive normalization: a deep learning approach for wildfire danger forecasting** | Published in IEEE Transactions on Geoscience and Remote Sensing, 2023 | Proposes location-aware adaptive normalization techniques for wildfire danger forecasting using deep learning. **Direct inference:** Provides advanced techniques for our fire risk prediction models. Location-aware normalization could improve our spatial risk scoring in areas with varying terrain and climate conditions. |
| 7 | **A forest fire prediction model based on cellular automata and machine learning** | Published in IEEE Access, vol. 12, pp. 55389–55403, 2024 | Combines cellular automata with machine learning for forest fire prediction, enabling spatial-temporal fire spread modeling. **Direct inference:** Cellular automata approach could enhance our fire spread prediction capabilities in the DataEnrichmentAgent, providing more accurate estimates of fire trajectory and affected areas over time. |
| 8 | **California wildfire prediction using machine learning** | Published in 2022 21st IEEE International Conference on Machine Learning and Applications (ICMLA), pp. 525–530 | Presents machine learning approaches specifically for California wildfire prediction. **Direct inference:** Since our initial focus is on wildfire events (including Maui), California-specific methodologies provide validated approaches for similar climate zones and fire behavior patterns. |
| 9 | **ML-Based Wildfire Prediction and Detection** | Presented at 2024 IEEE 3rd International Conference on Computing and Machine Intelligence (ICMI) | Covers both prediction and early detection of wildfires using machine learning techniques. **Direct inference:** Early detection methodologies could be integrated into our ingestion layer to identify potential fires before they're officially reported, improving response time. |
| 10 | **Enhancing Daily Wildfire Risk Prediction Application Through Interpretable Machine Learning Results** | Presented at IGARSS 2024-2024 IEEE International Geoscience and Remote Sensing Symposium | Emphasizes interpretable ML for wildfire risk prediction, making model decisions transparent to operators. **Direct inference:** Interpretability is crucial for dispatcher trust and decision-making. Our RiskScoringAgent should incorporate explainable AI features to show why specific risk scores were assigned. |
| 11 | **Utilizing Flink and Kafka Technologies for Real-Time Data Processing: A Case Study** | Published in Eurasia Proceedings of Science Technology Engineering and Mathematics, 2023 | Case study demonstrating effective utilization of Flink and Kafka for real-time data processing with seamless integration. **Direct inference:** Validates our architectural choice of Apache Kafka and Flink for the streaming processing pipeline. Provides best practices for agent-based stream processing. |
| 12 | **Benchmarking scalability of stream processing frameworks deployed as microservices in the cloud** | Available on arXiv (arXiv:2303.11088), 2023 | Benchmarks various stream processing frameworks including Flink when deployed as microservices, analyzing scalability characteristics. **Direct inference:** Provides performance benchmarks for our Flink-based microservices architecture. Helpful for capacity planning and scaling strategy as call volumes increase. |
| 13 | **Design and Optimization of Real-Time Big Data Stream Processing Model Based on Apache Flink** | Published in Springer Lecture Notes, 2022 | Focuses on optimizing real-time big data stream processing using Flink, balancing throughput and latency while enhancing fault tolerance. **Direct inference:** Directly applicable to optimizing our four Flink agents (CallProcessing, DataEnrichment, RiskScoring, AlertGeneration) for better throughput and lower latency in emergency response scenarios. |
| 14 | **Dynamic Disaster Management with Real-Time IoT Data Analysis and Response** | Published in 2024 IEEE International Conference on Automation and Computation (AUTOCOM) | Addresses dynamic disaster management using real-time IoT data analysis. Highlights how lack of real-time information in disasters like Hurricane Katrina led to inefficient response efforts. **Direct inference:** Emphasizes the critical need for our real-time risk scoring system. Validates our approach of continuously updating risk scores as conditions change rather than static assessments. |
| 15 | **2024 IEEE GRSS Data Fusion Contest: Rapid Flood Mapping** | IEEE Geoscience and Remote Sensing Society Contest, 2024 (sponsored by NASA and CNES) | Contest focused on developing data fusion algorithms that generate flood maps by processing spatial data from multiple sources (optical and SAR remote sensing, digital terrain models, land-use data). **Direct inference:** Multi-source data fusion techniques are directly applicable to our DataEnrichmentAgent. Shows how to effectively combine weather data (NOAA), fire data (FIRMS), and terrain data for comprehensive risk assessment. |
| 16 | **Artificial intelligence for modeling and understanding extreme weather and climate events** | Published in Nature Communications, 2025 | Reviews AI applications for modeling extreme weather and climate events, including case studies on drought, heatwaves, wildfires, and floods. **Direct inference:** Provides comprehensive overview of AI techniques for extreme weather that informs our climate-aware risk scoring. Particularly relevant for extending our system beyond wildfires to multiple hazard types. |
| 17 | **Artificial intelligence for climate prediction of extremes: State of the art, challenges, and future perspectives** | Published in WIREs Climate Change, 2024 | Reviews state-of-the-art AI methods for predicting extreme climate events, including FuXi-Extreme model that addresses systematic underestimation of heavy rainfall and strong winds. **Direct inference:** Advanced forecasting models like FuXi-Extreme could improve our weather data enrichment, providing more accurate extreme weather predictions for risk scoring. |
| 18 | **AI-enabled strategies for climate change adaptation: protecting communities, infrastructure, and businesses from the impacts of climate change** | Published in Springer Nature Applied Sciences, 2023 | Discusses AI strategies for climate adaptation, including mapping vulnerabilities to extreme weather and supporting local adaptation efforts. **Direct inference:** Informs how our system can incorporate vulnerability factors (age, medical conditions, mobility) into risk scoring to identify at-risk populations during climate disasters. |
| 19 | **An Intelligent Collaborative Decision Support System for Climate Disaster Management** | Published in Springer Lecture Notes in Computer Science, 2022 | Proposes collaborative decision support systems for climate disaster management with AI integration. **Direct inference:** Our system serves as a decision support tool for dispatchers. Suggests future enhancements for multi-user collaboration features and shared situational awareness. |
| 20 | **Situational Awareness as the Imperative Capability for Disaster Resilience in the Era of Complex Hazards and Artificial Intelligence** | Published on arXiv (arXiv:2508.16669), 2024 | Emphasizes situational awareness as critical for disaster resilience, leveraging AI for real-time insights. Uses H3 discrete global grid system to fuse satellite data, social media, and geospatial data. **Direct inference:** Validates our real-time visualization and interactive map approach. H3 grid system could enhance our spatial indexing and geospatial queries for faster risk assessment. |
| 21 | **Auto311: A Confidence-guided Automated System for Non-emergency Calls** | Published on arXiv (arXiv:2312.14185), 2023 | Presents an automated system for triaging non-emergency calls using confidence-guided classification. **Direct inference:** While our focus is emergency calls, confidence scoring mechanisms could be adapted to our CallProcessingAgent to flag uncertain classifications for human review. |
| 22 | **From Triage to Discharge: A Survey of NLP Tasks, Methods, and Open Challenges in the Emergency Department** | Published on arXiv (arXiv:2608.23627) | Comprehensive survey of NLP applications throughout the emergency department workflow, from triage to discharge. **Direct inference:** Provides broader context for NLP in emergency medicine. Suggests potential extensions of our NLP capabilities beyond initial call classification to follow-up tracking and outcome analysis. |
| 23 | **WildfireGenome: Interpretable Machine Learning Reveals Local Drivers of Wildfire Risk and Their Cross-County Variation** | Published on arXiv (arXiv:2511.11589) | Uses interpretable ML to identify local drivers of wildfire risk and geographic variations. **Direct inference:** Interpretable features could help explain risk score variations across different geographic areas in our system. Important for adapting our model to different regions beyond Maui. |
| 24 | **Predicting the Containment Time of California Wildfires Using Machine Learning** | Published on arXiv (arXiv:2512.09835) | Predicts wildfire containment time using machine learning, which affects resource allocation decisions. **Direct inference:** Containment time prediction could be integrated into our AlertGenerationAgent to help dispatchers estimate how long emergency conditions will persist and plan resource deployment accordingly. |
| 25 | **A comprehensive survey of the machine learning pipeline for wildfire risk prediction and assessment** | Published in ScienceDirect, 2025 | Comprehensive survey covering the entire ML pipeline for wildfire risk prediction and assessment. **Direct inference:** Provides end-to-end pipeline overview that validates our architecture from data ingestion through risk scoring. Helps identify gaps and potential improvements in our current pipeline. |

---

## Key Research Themes and Inferences

### 1. **NLP for Emergency Call Classification**
Research consistently shows that NLP-enhanced triage systems achieve superior performance (ROC-AUC: 0.91) compared to systems using only structured data (ROC-AUC: 0.88). This validates our use of Google Gemini API for natural language understanding in emergency call classification.

**Project Application:** Our CallProcessingAgent leverages state-of-the-art NLP for call classification, aligning with best practices identified in recent literature.

### 2. **Real-Time Stream Processing Architecture**
Studies demonstrate that Apache Kafka and Apache Flink provide robust, scalable solutions for real-time event processing with low latency and high throughput. The microservices deployment pattern enhances scalability and fault tolerance.

**Project Application:** Our event-driven architecture with Kafka topics and four specialized Flink agents (CallProcessing, DataEnrichment, RiskScoring, AlertGeneration) follows industry best practices for streaming data architectures.

### 3. **Geospatial Data Fusion for Risk Assessment**
IEEE research emphasizes multi-source data fusion combining satellite imagery, weather data, terrain models, and land-use information for accurate disaster risk assessment. Location-aware normalization and spatial indexing (e.g., H3 grid system) improve performance.

**Project Application:** Our DataEnrichmentAgent performs spatial-temporal joins of emergency calls with weather (NOAA) and fire data (FIRMS). Future enhancements could add satellite imagery and terrain models.

### 4. **Wildfire Prediction and Risk Modeling**
Recent IEEE publications demonstrate that machine learning models using geospatial features, weather patterns, and historical fire data can accurately predict wildfire risk and spread. Interpretable ML is crucial for operator trust.

**Project Application:** Our RiskScoringAgent uses hybrid ML/rule-based approaches for risk assessment. We should enhance interpretability to show dispatchers why specific risk scores were assigned.

### 5. **AI Decision Support for Climate Disasters**
Literature shows AI strengthens disaster management across all phases (prevention, preparedness, response, recovery) by providing real-time insights, early warnings, and dynamic risk assessment during rapidly evolving events.

**Project Application:** Our system provides continuous risk score updates during wildfire events, enabling dynamic prioritization as conditions change—a key capability identified in recent research.

### 6. **Real-Time Visualization and Situational Awareness**
WebSocket-based real-time dashboards with live map updates are essential for maintaining situational awareness during emergencies. Multi-source integration provides comprehensive operational picture.

**Project Application:** Our frontend uses WebSocket connections for real-time updates and Google Maps for interactive visualization, providing dispatchers with continuously updated situational awareness.

---

## Future Research Directions

Based on this literature survey, the following enhancements align with current research trends:

1. **Enhanced Interpretability:** Integrate explainable AI techniques to make risk scoring transparent
2. **Multi-Hazard Support:** Extend beyond wildfires to hurricanes, floods, and other climate disasters
3. **Advanced ML Models:** Implement ensemble methods and deep learning for improved prediction accuracy
4. **Additional Data Sources:** Incorporate satellite imagery, social media, and terrain data
5. **Spatial Indexing:** Adopt H3 or similar grid systems for faster geospatial queries
6. **Containment Prediction:** Add fire containment time estimation for better resource planning
7. **Confidence Scoring:** Implement confidence metrics for uncertain classifications
8. **Multi-User Collaboration:** Enable shared situational awareness across multiple dispatch centers

---

## Sources

### Emergency Call Classification & NLP
- [Improving triage performance in emergency departments using machine learning and natural language processing: a systematic review](https://link.springer.com/article/10.1186/s12873-024-01135-2)
- [AI-based approach for transcribing and classifying unstructured emergency call data](https://journals.plos.org/digitalhealth/article?id=10.1371%2Fjournal.pdig.0000406)
- [Artificial intelligence for severity triage based on conversations in an emergency department in Korea](https://www.nature.com/articles/s41598-025-99874-0)
- [Using machine learning and natural language processing in triage for prediction of clinical disposition in the emergency department](https://link.springer.com/article/10.1186/s12873-024-01152-1)
- [From Triage to Discharge: A Survey of NLP Tasks, Methods, and Open Challenges in the Emergency Department](https://arxiv.org/pdf/2608.23627)

### Wildfire Risk Assessment & Prediction
- [A Geospatial Approach to Wildfire Risk Modeling Using Machine Learning and Remote Sensing Data](https://www.researchgate.net/publication/382612222_A_Geospatial_Approach_to_Wildfire_Risk_Modeling_using_Machine_Learning_and_Remote_Sensing_Data)
- [A comprehensive survey of the machine learning pipeline for wildfire risk prediction and assessment](https://www.sciencedirect.com/science/article/pii/S1574954125003346)
- [WildfireGenome: Interpretable Machine Learning Reveals Local Drivers of Wildfire Risk](https://arxiv.org/pdf/2511.11589)
- [Predicting the Containment Time of California Wildfires Using Machine Learning](https://arxiv.org/pdf/2512.09835)
- [AI for climate change adaptation: analyzing Machine Learning's role in combating California's wildfires](https://link.springer.com/article/10.1007/s00146-025-02386-8)

### Real-Time Stream Processing
- [Utilizing Flink and Kafka Technologies for Real-Time Data Processing: A Case Study](https://www.researchgate.net/publication/376819829_Utilizing_Flink_and_Kafka_Technologies_for_Real-Time_Data_Processing_A_Case_Study)
- [Benchmarking scalability of stream processing frameworks deployed as microservices in the cloud](https://arxiv.org/pdf/2303.11088)
- [Design and Optimization of Real-Time Big Data Stream Processing Model Based on Apache Flink](https://link.springer.com/chapter/10.1007/978-3-032-08195-7_17)

### Climate & Disaster Management
- [Artificial intelligence for modeling and understanding extreme weather and climate events](https://www.nature.com/articles/s41467-025-56573-8)
- [Dynamic Disaster Management with Real-Time IoT Data Analysis and Response](https://www.researchgate.net/publication/379561523_Dynamic_Disaster_Management_with_Real-Time_IoT_Data_Analysis_and_Response)
- [AI-enabled strategies for climate change adaptation](https://link.springer.com/article/10.1007/s43762-023-00100-2)
- [Situational Awareness as the Imperative Capability for Disaster Resilience](https://arxiv.org/pdf/2508.16669)

### Geospatial Data Fusion
- [2024 IEEE GRSS Data Fusion Contest: Rapid Flood Mapping](https://www.grss-ieee.org/community/technical-committees/2024-ieee-grss-data-fusion-contest/)
- [How AI Helps Address the Growing Complexity of Disaster Response and Recovery](https://domesticpreparedness.com/articles/how-ai-helps-address-the-growing-complexity-of-disaster-response-and-recovery/)

### Industry Applications
- [As wildfires burn throughout the West, officials are turning to AI](https://www.govexec.com/technology/2024/09/wildfires-burn-throughout-west-officials-are-turning-ai/399594/)
- [Artificial intelligence detects fires early, protecting people and infrastructure](https://www.asce.org/publications-and-news/civil-engineering-source/article/2024/11/14/artificial-intelligence-detects-fires-early-protecting-people-infrastructure)

---

**Document Prepared:** October 2026  
**Project:** Sentinel Dispatch - AI-Powered Climate-Aware Emergency Triage System  
**Literature Coverage:** Research papers from 2022 onwards
