// Copyright (C) 2013 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.googlesource.gerrit.plugins.rabbitmq;

import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.TopicSubscriber;
import com.google.common.collect.Sets;
import com.google.common.flogger.FluentLogger;
import com.google.gerrit.extensions.annotations.PluginData;
import com.google.gerrit.extensions.annotations.PluginName;
import com.google.gerrit.extensions.events.LifecycleListener;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.extensions.registration.DynamicSet;
import com.google.gerrit.server.events.EventListener;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.Multibinder;
import com.googlesource.gerrit.plugins.rabbitmq.config.PluginProperties;
import com.googlesource.gerrit.plugins.rabbitmq.config.Properties;
import com.googlesource.gerrit.plugins.rabbitmq.config.PropertiesFactory;
import com.googlesource.gerrit.plugins.rabbitmq.config.section.AMQP;
import com.googlesource.gerrit.plugins.rabbitmq.config.section.Exchange;
import com.googlesource.gerrit.plugins.rabbitmq.config.section.General;
import com.googlesource.gerrit.plugins.rabbitmq.config.section.Gerrit;
import com.googlesource.gerrit.plugins.rabbitmq.config.section.Message;
import com.googlesource.gerrit.plugins.rabbitmq.config.section.Monitor;
import com.googlesource.gerrit.plugins.rabbitmq.config.section.Section;
import com.googlesource.gerrit.plugins.rabbitmq.config.section.Stream;
import com.googlesource.gerrit.plugins.rabbitmq.message.BaseProperties;
import com.googlesource.gerrit.plugins.rabbitmq.message.BasePropertiesProvider;
import com.googlesource.gerrit.plugins.rabbitmq.message.GerritEventPublisher;
import com.googlesource.gerrit.plugins.rabbitmq.message.GerritEventPublisherFactory;
import com.googlesource.gerrit.plugins.rabbitmq.message.GsonProvider;
import com.googlesource.gerrit.plugins.rabbitmq.message.Publisher;
import com.googlesource.gerrit.plugins.rabbitmq.rest.RestModule;
import com.googlesource.gerrit.plugins.rabbitmq.session.PublisherSession;
import com.googlesource.gerrit.plugins.rabbitmq.session.SubscriberSession;
import com.googlesource.gerrit.plugins.rabbitmq.session.type.AMQPPublisherSession;
import com.googlesource.gerrit.plugins.rabbitmq.session.type.SubscriberSessionFactoryImpl;
import com.googlesource.gerrit.plugins.rabbitmq.worker.DefaultEventWorker;
import com.googlesource.gerrit.plugins.rabbitmq.worker.EventWorker;
import com.googlesource.gerrit.plugins.rabbitmq.worker.EventWorkerFactory;
import com.googlesource.gerrit.plugins.rabbitmq.worker.UserEventWorker;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.util.FS;

class Module extends AbstractModule {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private final RabbitMqBrokerApiModule rabbitMqBrokerApiModule;
  private final boolean brokerApiEnabled;
  private Set<TopicSubscriber> activeConsumers = Sets.newHashSet();

  @Inject
  public Module(
      RabbitMqBrokerApiModule rabbitMqBrokerApiModule,
      @PluginName final String pluginName,
      @PluginData final File pluginData) {
    this.rabbitMqBrokerApiModule = rabbitMqBrokerApiModule;
    this.brokerApiEnabled =
        getBaseConfig(pluginName, pluginData).getBoolean("General", "enableBrokerApi", false);
  }

  @Inject(optional = true)
  public void setPreviousBrokerApi(DynamicItem<BrokerApi> previousBrokerApi) {
    if (previousBrokerApi != null && previousBrokerApi.get() != null) {
      BrokerApi api = previousBrokerApi.get();
      this.activeConsumers = api.topicSubscribers();
    }
  }

  @Override
  protected void configure() {

    Multibinder<Section> sectionBinder = Multibinder.newSetBinder(binder(), Section.class);
    sectionBinder.addBinding().to(AMQP.class);
    sectionBinder.addBinding().to(Exchange.class);
    sectionBinder.addBinding().to(Gerrit.class);
    sectionBinder.addBinding().to(Message.class);
    sectionBinder.addBinding().to(Monitor.class);
    sectionBinder.addBinding().to(General.class);
    sectionBinder.addBinding().to(Stream.class);

    install(
        new FactoryModuleBuilder()
            .implement(PublisherSession.class, AMQPPublisherSession.class)
            .build(AMQPPublisherSession.Factory.class));
    install(
        new FactoryModuleBuilder()
            .implement(Publisher.class, GerritEventPublisher.class)
            .build(GerritEventPublisherFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(Properties.class, PluginProperties.class)
            .build(PropertiesFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(EventWorker.class, UserEventWorker.class)
            .build(EventWorkerFactory.class));
    bind(SubscriberSession.Factory.class)
        .to(SubscriberSessionFactoryImpl.class)
        .in(Singleton.class);
    bind(Gson.class).toProvider(GsonProvider.class).in(Singleton.class);

    DynamicSet.bind(binder(), LifecycleListener.class).to(Manager.class);
    DynamicSet.bind(binder(), EventListener.class).to(DefaultEventWorker.class);

    bind(Properties.class)
        .annotatedWith(BaseProperties.class)
        .toProvider(BasePropertiesProvider.class);
    bind(new TypeLiteral<Set<TopicSubscriber>>() {}).toInstance(activeConsumers);

    if (brokerApiEnabled) {
      install(rabbitMqBrokerApiModule);
      install(new RestModule());
    } else {
      logger.atInfo().log(
          "The RabbitMqBrokerApi is disabled, set enableBrokerApi to true if you want to enable"
              + " it");
    }
  }

  private static FileBasedConfig getBaseConfig(String pluginName, File pluginData) {
    File baseConfigFile =
        pluginData.toPath().resolve(pluginName + BasePropertiesProvider.FILE_EXT).toFile();
    FileBasedConfig config = new FileBasedConfig(baseConfigFile, FS.DETECTED);
    try {
      config.load();
    } catch (IOException | ConfigInvalidException e) {
      logger.atInfo().withCause(e).log("Unable to load %s", baseConfigFile.getAbsolutePath());
    }
    return config;
  }
}
