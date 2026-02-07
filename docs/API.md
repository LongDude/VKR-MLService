# 1. Redis
metadata_queue: Stream - поток статей на загрузку в временное хранилище
    id - arXive:<id>
    title - Название статьи
    authors - список авторов
    abstract - аннотация
    categories - категории из arXive


# 2. Minio
## 2.1. После первого запуска контейнера:
1. Подключаемся через docker exec к контейнеру
2. Проводим предварительную инициализацию бакетов:
    ./etc/minio-setup.sh
~~3. Создаем пользователя и ключи доступа~~
    ```sh
    # Создание пользователя
    mc admin user add myminio <MINIO_USER> <MINIO_PASSWORD>
    
    # Прописываем права пользователю
    mc admin policy attach myminio readwrite --user <MINIO_USER>
    
    # Генерируем ключи доступа
    mc admin user svcacct add myminio <MINIO_USER>

    # Полуаем ответ в формате
    # Access Key: <MINIO_ACCESS_KEY>
    # Secret Key: <MINIO_SECRET_KEY>
    # Expiration: no-expiry
    # Сохраняем ключи в .env
    ```
3. ACCESS_KEYS устарели: рекомендуется исползовать ROOT_USER и ROOT_PASSWORD
4. Сохраняем ключи и исползованные данные пользователя в `.env`, см. `.env.example`