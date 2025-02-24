# Telegram - Data Pipeline  

## Architecture

<p align="center"><img src="image/map api telegram.png"></p>

<p>
### No chatbot é onde estão os dados transacionais gerados no grupo do Telegram para atendimento aos usuários e clientes de uma empresas fictícia;
### Na 1ª etapa de ingestão os dados crus saem do Bot e passam pela AWS API Gateway através de uma url;
### Posteriormente uma função da AWS Lambda processa esses dados crus no formato JSON e armazena-os nesse mesmo formato em um AWS Bucket S3(cru);
### Já na 2ª etapa ETL outra função Lambda capta os dados crus transforma-os em dados enriquecidos e armazena-os em outro Bucket S3(enriquecido)  `parquet`, 
### no entanto, antes disso todos os dias é acionado um Event Bridge que verifica se existem novos dados crus no Bucket S3(cru) , se houver refaz toda a etapa de ETL novamente e armazena os novos dados no Bucket S3(enriquecido);
### Na 3ª etapa AWS Athena(etapa de consulta e geração de insights) são gerados consultas SQL em tabelas que possuem os dados que foram tratados do Bucket S3(enriquecido).
### Tudo isso permite a automatização de um sistema que possibilita a geração de valor e controle de qualidade de atendimento a usuários e clientes de várias empresas mundo afora!
### Imagine grandes empresas onde megabytes, gigabytes ou até mesmo terabytes  de dados são gerados diarimente em sistemas de atendimento, sendo assim, somente um projeto como esse poderia lidar com tantos dados e ser capas de gerar valor e insights relevantes para a tomada de decisões importantes que alavanquem ainda mais os negócios empresariais e de outros setores!
### Além do mais é importante destacar que a AWS é uma das empresas possuí os serviços mais baratos e robustos no que tange ao armazenamento e consumo de dados!
</p>


## Tools used

![Telegram](https://img.shields.io/badge/-Telegram_Bots-blue?style=flat-square&logo=telegram) 
![Amazon AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/Python-white?style=flat-square&logo=python)
![SQL](https://img.shields.io/badge/-SQL-blue?style=flat-square&logo=sqlite)


