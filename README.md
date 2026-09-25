# Trabalho1-RodrigoPerobadeSouza

Para uso do sistema é necessário ter o Docker instalado com o Kubernetes ativado ( Ativa dentro das configurações do Docker). Com os dois programas instalados, rode init.bat e o sistema entrara no ar.

Na arquitetura temos o script "produtor.py" gerando mensagens e direcionando para um docker Kafka, onde essas mensagens serão lidas pelo script "consumidor.py". O "consumidor.py", além de ler as mensagens, as analisa para conferir se os dados esstão dentro das medidas esperadas e grava uma entrada no banco de dados com os valores lidos. Há também um container rodando MySQL para servir como banco de dados.

Para simular falhas temos o container com o script "produtorComDefeito.py" que inicialmente enviará mensagens dentro dos limites aceitáveis mas com o tempo começará a enviar mensagens com erros (Temperatura fora da margem aceita).

Não consegui testar a elasticidade pois tive problemas com o metrics-server e ele não está rodando na minha máquina

Para rebalenciamento, o kubernetes gerencia caso algum pod inicial falhe e inicia um novo o substituindo