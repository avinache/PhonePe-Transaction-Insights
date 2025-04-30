# PhonePe-Transaction-Insights
Conduct a comprehensive analysis of transaction patterns, user engagement trends, and insurance-related metrics on the PhonePe platform.


India's digital payments landscape has garnered global attention, evolving rapidly across both metropolitan centers and remote rural areas. This transformation is fueled by widespread mobile penetration, affordable internet access, and a robust public digital infrastructure, spearheaded by the government and the Reserve Bank of India. Launched in 2016, PhonePe has significantly benefited from this API-driven digital ecosystem.
In light of this growing reliance on platforms like PhonePe, our analysis will delve into transaction patterns, user engagement levels, and insurance-linked data. The study will feature visual representations of payment trends across various states and districts, with an emphasis on highlighting top-performing regions and PIN codes.

To initiate our analysis, we begin by accessing the dataset available via a Git repository. This dataset is well-structured and categorized into three primary segments—Transactions, Users, and Insurance—based on the data available under the Explore tab of PhonePe Pulse.

The data is organized into three main formats:

Aggregated: Consolidated values for various payment categories listed under the Categories section.
Map: Geographical distribution at both the state and district levels.
Top: Highlights the top-performing states, districts, and PIN codes.

Each of these segments contains subfolders for Transactions, Users, and Insurance. Within the main India directory, data is further divided by year (for country-level insights) and a separate state folder, which contains yearly data for all individual states.
At both the national and state levels, data is further broken down by quarters—each year folder includes up to four files, named 1 to 4, representing Q1 to Q4 respectively.

The initial step of my analysis involved extracting data from the Git repository. I began by cloning the repository into my local working directory. Subsequently, I retrieved data from the three primary sections—Aggregated, Map, and Top—focusing on Transactions, Users, and Insurance across various years, states, and quarters. This process resulted in the creation of nine distinct tables.

In the second step, I performed a review of the extracted data and proceeded to load all extracted tables into a MySQL database using appropriate SQL queries. With the data successfully imported, I began analyzing it in alignment with the specified business use cases.

> Decoding Transaction Dynamics on PhonePe
> Device Dominance and User Engagement Analysis
> User Engagement and Growth Strategy
> Insurance Engagement Analysis
> Transaction Analysis Across States and Districts

The third step of my analysis focused on visualizing the insights derived from the business use case. To accomplish this, I employed various Python libraries such as Matplotlib, Plotly, and Seaborn well known tools for generating static, animated, and interactive visualizations that are especially effective for graphing and data analysis.

In the fourth step, I utilized the Streamlit library to transform these visualizations into an interactive web-based application. Streamlit enabled me to build a user-friendly and dynamic interface for exploring the analytical outcomes with minimal coding effort.

In the fifth step of my analysis, I developed interactive visualization dashboards using Power BI, Microsoft’s business analytics platform. This tool facilitated the creation of dynamic reports and dashboards, enabling effective data visualization, insight sharing, and informed decision-making aligned with the business use case.

The insights derived from this comprehensive analysis offer a deep understanding of transactional patterns, user engagement, and insurance-related activities on the PhonePe platform.
