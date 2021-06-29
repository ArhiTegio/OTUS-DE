# VerticaVM, StreamSets и Tableau на данный StreamMeetUp
## Как установить

1. Устанавливается wsl2
2. Устанавливается [Docker Compose](https://docs.docker.com/compose/install/)
3. Устанавливается Oracle VM VirtualBox (https://download.virtualbox.org/virtualbox/6.1.22/VirtualBox-6.1.22-144080-Win.exe)
4. Скачивается образ Vertica (https://www.vertica.com/)
5. Установить Tableau (https://www.tableau.com/products/desktop/download?signin=25d4c6606f7fe8ee683f62b016048849)
6. Клонируется репозиторий `git clone`
7. Открывается папка
8. Запускается контейнер `docker-compose up`
9. Определить id контейнера со streamsets `docker ps -aqf "name=course_streamsets_1"`
10. Скачивает по инструкции JDBC драйвера для Vertica (https://www.vertica.com/docs/10.1.x/HTML/Content/Authoring/ConnectingToVertica/ClientJDBC/InstallingJDBCDriverOnLinuxSolarisAIXAndHPUX.htm)
11. Разархивируем в контейнере course_streamsets_1 в папке /opt и прописать class (export CLASSPATH=/opt/vertica/java/lib/vertica-jdbc-10.1.1-0.jar)
12. Запустить виртуальную машину Vertica 


Стандартный логин и пароль для StreamSets: admin / admin


